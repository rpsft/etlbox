using System;
using System.Collections;
using System.Collections.Generic;
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

        var (type, instance) = MapToTyped((IDictionary<string, object>)row);

        // Typed array so AsQueryable sees the runtime element type.
        // "new[] { instance }" gives object[] and Where() loses property typing.
        var array = Array.CreateInstance(type, 1);
        array.SetValue(instance, 0);

        return array.AsQueryable().Any(s_parsingConfig, FilterExpression);
    }

    // Recursively builds a runtime DynamicClass mirroring the dictionary structure:
    //   - nested IDictionary  -> nested DynamicClass property (recursion)
    //   - homogeneous IEnumerable -> List<elementType> (collection of dicts or scalars)
    //   - everything else -> kept as-is via GetType() / typeof(object) fallback
    private static (Type Type, object Instance) MapToTyped(IDictionary<string, object> dict)
    {
        var properties = new List<DynamicProperty>(dict.Count);
        var preparedValues = new Dictionary<string, object>(dict.Count);

        foreach (var pair in dict)
        {
            if (pair.Value is IDictionary<string, object> nestedDict)
            {
                var (nestedType, nestedInstance) = MapToTyped(nestedDict);
                properties.Add(new DynamicProperty(pair.Key, nestedType));
                preparedValues[pair.Key] = nestedInstance;
            }
            else if (TryAsCollection(pair.Value, out var items))
            {
                var (listType, listInstance) = MapCollection(items);
                properties.Add(new DynamicProperty(pair.Key, listType));
                preparedValues[pair.Key] = listInstance;
            }
            else
            {
                properties.Add(
                    new DynamicProperty(pair.Key, pair.Value?.GetType() ?? typeof(object))
                );
            }
        }

        var type = DynamicClassFactory.CreateType(properties);
        var instance = Activator.CreateInstance(type)!;

        foreach (var pair in dict)
        {
            var value = preparedValues.TryGetValue(pair.Key, out var prepared)
                ? prepared
                : pair.Value;
            type.GetProperty(pair.Key)?.SetValue(instance, value);
        }

        return (type, instance);
    }

    private static bool TryAsCollection(object? value, out IList<object> items)
    {
        // string is IEnumerable<char> but we treat it as scalar; byte[] is also kept opaque.
        if (value is null or string or byte[])
        {
            items = null!;
            return false;
        }

        if (value is IEnumerable enumerable)
        {
            items = enumerable.Cast<object>().ToList();
            return true;
        }

        items = null!;
        return false;
    }

    // Builds a typed List<T> from a homogeneous collection.
    // Throws if items have inconsistent shapes (different field sets / types).
    private static (Type ListType, object ListInstance) MapCollection(IList<object> items)
    {
        if (items.Count == 0)
        {
            // Empty collection: fall back to List<object>. Count() / .Any() work,
            // .Any(predicate) won't (no element type properties).
            var emptyType = typeof(List<object>);
            return (emptyType, Activator.CreateInstance(emptyType)!);
        }

        var first = items[0];
        Type elementType;
        Func<object, object> projectItem;

        if (first is IDictionary<string, object> firstDict)
        {
            elementType = MapToTyped(firstDict).Type;
            projectItem = item =>
            {
                if (item is not IDictionary<string, object> d)
                    throw new InvalidOperationException(
                        "Heterogeneous collection: mix of dictionary and non-dictionary items."
                    );
                var (itemType, itemInstance) = MapToTyped(d);
                if (itemType != elementType)
                    throw new InvalidOperationException(
                        "Heterogeneous collection: items have different field sets or types."
                    );
                return itemInstance;
            };
        }
        else
        {
            elementType = first?.GetType() ?? typeof(object);
            projectItem = item => item!;
        }

        var listType = typeof(List<>).MakeGenericType(elementType);
        var listInstance = (IList)Activator.CreateInstance(listType)!;
        foreach (var item in items)
        {
            listInstance.Add(item is null ? null : projectItem(item));
        }

        return (listType, listInstance);
    }
}
