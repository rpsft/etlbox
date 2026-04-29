using System;
using System.Dynamic;
using System.Linq;
using System.Linq.Dynamic.Core;
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
    // ConvertObjectToSupportComparison enables operators on properties typed as object
    // (handles null-valued fields and mixed numeric literals like "X > 0" with X=decimal).
    protected static readonly ParsingConfig s_parsingConfig =
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

    protected virtual bool EvaluateExpression(TInput row)
    {
        if (string.IsNullOrWhiteSpace(FilterExpression))
            throw new InvalidOperationException("FilterExpression is not set.");

        // For typed TInput "new[] { row }" preserves the compile-time element type,
        // AsQueryable sees TInput, Dynamic LINQ resolves properties via PropertyInfo.
        return new[] { row }.AsQueryable().Any(s_parsingConfig, FilterExpression);
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

        // Typed array so AsQueryable sees the runtime element type.
        // "new[] { instance }" gives object[] and Where() loses property typing.
        var array = Array.CreateInstance(type, 1);
        array.SetValue(instance, 0);

        return array.AsQueryable().Any(s_parsingConfig, FilterExpression);
    }
}
