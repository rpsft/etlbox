using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Dynamic.Core;

namespace ALE.ETLBox.DynamicLinq;

/// <summary>
/// Maps an <see cref="ExpandoObject"/> to a runtime DynamicClass instance with typed
/// properties so that <c>System.Linq.Dynamic.Core</c> can resolve property names
/// against a real CLR type.
/// </summary>
/// <remarks>
/// <para>
/// Recursively handles nested <see cref="IDictionary{TKey,TValue}"/>, homogeneous
/// collections (lists of dictionaries or scalars), custom classes (kept as-is and
/// resolved via <c>PropertyInfo</c>) and <c>null</c> values (typed as <c>object</c>).
/// </para>
/// <para>
/// Types are emitted into a shared persistent <c>AssemblyBuilder</c> via
/// <c>DynamicClassFactory</c> and cached by property signature. Repeated rows of the
/// same shape reuse the same emitted type, so the amortised cost is low. There is no
/// <c>Assembly.Load(bytes)</c> per shape.
/// </para>
/// <para>
/// <c>string</c> and <c>byte[]</c> are deliberately treated as scalar values rather
/// than as <c>IEnumerable</c> collections. Heterogeneous collections (items with
/// different field sets or types) throw <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// Internal API - used by <see cref="ExpressionRowFiltration"/>. Not part of the
/// public surface.
/// </para>
/// </remarks>
internal static class ExpandoTypeMapper
{
    /// <summary>
    /// Builds a runtime DynamicClass type for the row and returns an instance populated
    /// from the row values.
    /// </summary>
    /// <param name="row">Row to map. Field values may be scalars, nested
    /// <c>IDictionary&lt;string, object&gt;</c>, custom classes or homogeneous
    /// collections.</param>
    /// <returns>The emitted runtime type and an instance with all fields assigned.</returns>
    public static (Type Type, object Instance) Map(ExpandoObject row) =>
        MapToTyped((IDictionary<string, object>)row);

    private static (Type Type, object Instance) MapToTyped(IDictionary<string, object> dict)
    {
        var properties = new DynamicProperty[dict.Count];
        var values = new object?[dict.Count];

        var i = 0;
        foreach (var pair in dict)
        {
            var (propertyType, value) = ResolveProperty(pair.Value);
            properties[i] = new DynamicProperty(pair.Key, propertyType);
            values[i] = value;
            i++;
        }

        var type = DynamicClassFactory.CreateType(properties);
        var instance = Activator.CreateInstance(type)!;

        i = 0;
        foreach (var pair in dict)
        {
            type.GetProperty(pair.Key)?.SetValue(instance, values[i++]);
        }

        return (type, instance);
    }

    private static (Type Type, object? Value) ResolveProperty(object? raw)
    {
        if (raw is IDictionary<string, object> nestedDict)
        {
            var (nestedType, nestedInstance) = MapToTyped(nestedDict);
            return (nestedType, nestedInstance);
        }

        if (TryAsCollection(raw, out var items))
        {
            var (listType, listInstance) = MapCollection(items);
            return (listType, listInstance);
        }

        return (raw?.GetType() ?? typeof(object), raw);
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
