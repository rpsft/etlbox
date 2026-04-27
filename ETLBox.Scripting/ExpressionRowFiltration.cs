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
/// Nested ExpandoObject / IDictionary fields are mapped recursively, custom classes
/// are resolved through PropertyInfo, homogeneous collections become typed List&lt;T&gt;
/// (enabling Any(predicate), Count(), Sum(selector), Contains(...) in expressions).
/// Heterogeneous collections (items with different shapes) are not supported.
/// </summary>
[PublicAPI]
public class ExpressionRowFiltration : RowFiltration<ExpandoObject>
{
    // ConvertObjectToSupportComparison enables operators on properties typed as object
    // (null-valued fields fall back to typeof(object); also handles mixed numeric literals).
    private static readonly ParsingConfig s_parsingConfig =
        new() { ConvertObjectToSupportComparison = true };

    /// <summary>
    /// String expression to evaluate for each row.
    /// Field names are resolved from ExpandoObject properties.
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

    public ExpressionRowFiltration([CanBeNull] ILogger<ExpressionRowFiltration> logger)
        : base(logger)
    {
        PredicateFunc = EvaluateExpression;
    }

    private bool EvaluateExpression(ExpandoObject row)
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
