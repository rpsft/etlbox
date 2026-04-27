using System;
using System.Dynamic;
using System.Linq;
using System.Linq.Dynamic.Core;
using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.Scripting;

/// <summary>
/// Filters data rows based on a string expression evaluated via System.Linq.Dynamic.Core.
/// Supports field comparisons, arithmetic, logical operators, null checks.
/// Generic version for typed POCOs - properties are resolved through PropertyInfo
/// directly, no runtime type generation needed.
/// </summary>
/// <typeparam name="TInput">Row type. Properties referenced in FilterExpression
/// must be public.</typeparam>
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
/// Filters ExpandoObject rows by a string expression.
/// Nested ExpandoObject / IDictionary fields are mapped recursively, custom classes
/// are resolved through PropertyInfo, homogeneous collections become typed List&lt;T&gt;
/// (enabling Any(predicate), Count(), Sum(selector), Contains(...) in expressions).
/// Heterogeneous collections (items with different shapes) are not supported.
/// </summary>
/// <see cref="ExpressionRowFiltration{TInput}"/>
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
