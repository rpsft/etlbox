using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Dynamic.Core.CustomTypeProviders;
using System.Linq.Expressions;
using System.Reflection;
using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.DynamicLinq;

/// <summary>
/// Filters rows by a string expression parsed and compiled by
/// <c>System.Linq.Dynamic.Core</c>. Suited for XML-defined flows where the predicate
/// is configured in the package, not in C# code. No Roslyn, no per-shape assembly
/// emission, no <c>Assembly.Load</c>.
/// </summary>
/// <typeparam name="TInput">
/// Row type. Properties referenced in <see cref="FilterExpression"/> must be public.
/// For typed <c>TInput</c> property names are resolved through <c>PropertyInfo</c>
/// directly; no runtime type generation is needed.
/// </typeparam>
/// <remarks>
/// <para>
/// Supported in the expression: comparisons (<c>==</c>, <c>!=</c>, <c>&gt;</c>, <c>&lt;</c>,
/// <c>&gt;=</c>, <c>&lt;=</c>), logical operators (<c>&amp;&amp;</c>, <c>||</c>, <c>!</c>),
/// arithmetic (<c>+</c>, <c>-</c>, <c>*</c>, <c>/</c>, <c>%</c>), member access on nested
/// objects, null checks, and LINQ-style methods on homogeneous collections
/// (<c>Items.Any(predicate)</c>, <c>Items.Count()</c>, <c>Items.Sum(selector)</c>,
/// <c>Items.Contains(value)</c>).
/// </para>
/// <para>
/// For the broader picture - when to pick this over <c>ScriptedRowTransformation</c>,
/// limitations on heterogeneous collections, internals of the
/// <see cref="ExpressionRowFiltration"/> non-generic form, supported value types - see
/// <c>docs/dataflow/row-filtration.md</c>.
/// </para>
/// <para>
/// The <see cref="ParsingConfig.ConvertObjectToSupportComparison"/> flag is enabled so
/// comparison operators work on properties typed as <c>object</c> (null-valued fields
/// and mixed numeric literals such as <c>Reserve &gt; 0</c> when <c>Reserve</c> is
/// <c>decimal</c>).
/// </para>
/// </remarks>
/// <example>
/// <code>
/// ExpressionRowFiltration&lt;ChangeRatioRow&gt; filtration = new ExpressionRowFiltration&lt;ChangeRatioRow&gt;(
///     "AdminReserveRatioPrevious != AdminReserveRatio");
/// source.LinkTo(filtration);
/// filtration.LinkTo(destination);
/// </code>
/// </example>
[PublicAPI]
public class ExpressionRowFiltration<TInput> : RowFiltration<TInput>
{
    /// <summary>
    /// Parser configuration applied to every <see cref="FilterExpression"/> evaluation.
    /// Default has <see cref="ParsingConfig.ConvertObjectToSupportComparison"/> = true
    /// (enables operators on properties typed as <c>object</c>, handles null-valued
    /// fields and mixed numeric literals like <c>"X &gt; 0"</c> when <c>X</c> is decimal).
    /// </summary>
    /// <remarks>
    /// Mutate this instance to register additional types or change parser flags. For
    /// the typical "let me call methods on my types" case use <see cref="RegisterCustomTypes"/>.
    /// Reassigning the property to a fresh <see cref="ParsingConfig"/> is also supported.
    /// </remarks>
    public ParsingConfig ParsingConfig { get; set; } =
        new() { ConvertObjectToSupportComparison = true };

    /// <summary>
    /// String expression to evaluate for each row.
    /// Field names are resolved from TInput properties.
    /// Supports: ==, !=, >, &lt;, >=, &lt;=, &amp;&amp;, ||, !, +, -, *, /, %
    /// </summary>
    public string FilterExpression { get; set; } = string.Empty;

    public ExpressionRowFiltration()
    {
        PredicateFunc = EvaluateExpression;
    }

    public ExpressionRowFiltration(string filterExpression)
        : this()
    {
        FilterExpression = filterExpression;
    }

    public ExpressionRowFiltration([CanBeNull] ILogger<ExpressionRowFiltration<TInput>> logger)
        : base(logger)
    {
        PredicateFunc = EvaluateExpression;
    }

    /// <summary>
    /// Register user types so their public instance methods become callable from
    /// <see cref="FilterExpression"/>. Without this, expressions like
    /// <c>"MyDto.SomeMethod() == 1"</c> fail to parse on user-defined types.
    /// Built-in types (<c>string</c>, <c>DateTime</c>, framework types) work without
    /// registration.
    /// </summary>
    /// <remarks>
    /// Combines with any types already registered on <see cref="ParsingConfig"/>;
    /// existing <c>CustomTypeProvider</c> is replaced with one that exposes the union.
    /// </remarks>
    /// <example>
    /// <code>
    /// var filtration = new ExpressionRowFiltration&lt;Order&gt;("Customer.IsPremium() &amp;&amp; Total &gt; 100");
    /// filtration.RegisterCustomTypes(typeof(Customer));
    /// </code>
    /// </example>
    public void RegisterCustomTypes(params Type[] types)
    {
        if (types is null || types.Length == 0)
            return;

        var existing = ParsingConfig.CustomTypeProvider?.GetCustomTypes() ?? new HashSet<Type>();
        var merged = new HashSet<Type>(existing);
        foreach (var t in types.Where(t => t is not null))
            merged.Add(t);

        ParsingConfig.CustomTypeProvider = new InlineCustomTypeProvider(merged);

        // Type provider change invalidates the compiled cache - any registered type
        // could change how an expression resolves.
        InvalidateCompiledCache();
    }

    // Compiled-delegate cache. Holds the last successful Compile() output for
    // FilterExpression + ParsingConfig pair. The TInput-typed path produces a
    // direct Func<TInput, bool>; subsequent EvaluateExpression calls invoke the
    // delegate without re-parsing or building a Queryable per row.
    private Func<TInput, bool>? _compiledPredicate;
    private string? _compiledForExpression;
    private ParsingConfig? _compiledForConfig;

    /// <summary>
    /// Drops the cached compiled delegate, forcing a recompile on the next
    /// <see cref="EvaluateExpression"/> call. Useful when the user mutates
    /// <see cref="ParsingConfig"/> in place; reassigning the property is
    /// detected automatically.
    /// </summary>
    public void InvalidateCompiledCache()
    {
        _compiledPredicate = null;
        _compiledForExpression = null;
        _compiledForConfig = null;
    }

    protected virtual bool EvaluateExpression(TInput row)
    {
        if (string.IsNullOrWhiteSpace(FilterExpression))
            throw new InvalidOperationException("FilterExpression is not set.");

        EnsureCompiled();
        return _compiledPredicate!(row);
    }

    private void EnsureCompiled()
    {
        if (
            _compiledPredicate is not null
            && _compiledForExpression == FilterExpression
            && ReferenceEquals(_compiledForConfig, ParsingConfig)
        )
        {
            return;
        }

        var lambda = DynamicExpressionParser.ParseLambda<TInput, bool>(
            ParsingConfig,
            createParameterCtor: false,
            FilterExpression
        );
        _compiledPredicate = lambda.Compile();
        _compiledForExpression = FilterExpression;
        _compiledForConfig = ParsingConfig;
    }

    private sealed class InlineCustomTypeProvider : IDynamicLinqCustomTypeProvider
    {
        private readonly HashSet<Type> _types;

        public InlineCustomTypeProvider(HashSet<Type> types) => _types = types;

        public HashSet<Type> GetCustomTypes() => _types;

        public Dictionary<Type, List<MethodInfo>> GetExtensionMethods() => new();

        public Type? ResolveType(string typeName) =>
            _types.FirstOrDefault(t => t.FullName == typeName || t.Name == typeName);

        public Type? ResolveTypeBySimpleName(string simpleTypeName) =>
            _types.FirstOrDefault(t => t.Name == simpleTypeName);
    }
}

/// <summary>
/// Non-generic <see cref="ExpressionRowFiltration{TInput}"/> bound to
/// <see cref="ExpandoObject"/>. Used by the XML reader and by flows on dynamic rows.
/// </summary>
/// <remarks>
/// <para>
/// Each row is mapped to a runtime DynamicClass before evaluation: nested
/// <c>IDictionary&lt;string, object&gt;</c> fields are mapped recursively, custom
/// classes are kept as-is and resolved through <c>PropertyInfo</c>, homogeneous
/// collections become typed <c>List&lt;T&gt;</c> (enabling
/// <c>Any(predicate)</c>, <c>Count()</c>, <c>Sum(selector)</c>, <c>Contains(...)</c>
/// in expressions). The mapping is performed by <see cref="ExpandoTypeMapper"/>.
/// </para>
/// <para>
/// The mapped instance is wrapped in an array via <c>Array.CreateInstance(type, 1)</c>
/// - required because <c>new[] { instance }</c> would give <c>object[]</c> and
/// <c>AsQueryable()</c> would lose the runtime element type, breaking property
/// resolution in the parsed expression.
/// </para>
/// <para>
/// Heterogeneous collections (items with different field sets or types) throw at
/// evaluation time. See <c>docs/dataflow/row-filtration.md</c> for the full list of
/// limitations.
/// </para>
/// </remarks>
/// <example>
/// XML form:
/// <code language="xml">
/// &lt;ExpressionRowFiltration&gt;
///     &lt;FilterExpression&gt;Reserve &amp;gt; 0 &amp;amp;&amp;amp; Type == &amp;quot;Day&amp;quot;&lt;/FilterExpression&gt;
/// &lt;/ExpressionRowFiltration&gt;
/// </code>
/// Programmatic form:
/// <code>
/// ExpressionRowFiltration filtration = new ExpressionRowFiltration(
///     "Reserve > 0 &amp;&amp; Type == \"Day\"");
/// </code>
/// </example>
/// <seealso cref="ExpressionRowFiltration{TInput}"/>
[PublicAPI]
public class ExpressionRowFiltration : ExpressionRowFiltration<ExpandoObject>
{
    // Compiled-delegate cache for the ExpandoObject path. The cache key is
    // (FilterExpression, ParsingConfig, mapped DynamicClass type). When a row
    // arrives with the same shape as the cached one, we skip the parse/wrap
    // step and invoke the previously compiled delegate on the freshly mapped
    // instance.
    private Func<object, bool>? _expandoCompiledPredicate;
    private string? _expandoCompiledForExpression;
    private ParsingConfig? _expandoCompiledForConfig;
    private Type? _expandoCompiledForType;

    public ExpressionRowFiltration() { }

    public ExpressionRowFiltration(string filterExpression)
        : base(filterExpression) { }

    public ExpressionRowFiltration([CanBeNull] ILogger<ExpressionRowFiltration> logger)
        : base(logger) { }

    protected override bool EvaluateExpression(ExpandoObject row)
    {
        if (string.IsNullOrWhiteSpace(FilterExpression))
            throw new InvalidOperationException("FilterExpression is not set.");

        var (type, instance) = ExpandoTypeMapper.Map(row);

        if (
            _expandoCompiledPredicate is null
            || _expandoCompiledForExpression != FilterExpression
            || !ReferenceEquals(_expandoCompiledForConfig, ParsingConfig)
            || _expandoCompiledForType != type
        )
        {
            // Build a Func<object, bool> wrapper around the type-specific lambda
            // so we can invoke it without DynamicInvoke (slow) and without
            // wrapping each row in an Array.CreateInstance + AsQueryable per call.
            var lambda = DynamicExpressionParser.ParseLambda(
                ParsingConfig,
                type,
                typeof(bool),
                FilterExpression
            );
            var paramObj = Expression.Parameter(typeof(object), "o");
            var wrapped = Expression.Lambda<Func<object, bool>>(
                Expression.Invoke(lambda, Expression.Convert(paramObj, type)),
                paramObj
            );

            _expandoCompiledPredicate = wrapped.Compile();
            _expandoCompiledForExpression = FilterExpression;
            _expandoCompiledForConfig = ParsingConfig;
            _expandoCompiledForType = type;
        }

        return _expandoCompiledPredicate(instance);
    }
}
