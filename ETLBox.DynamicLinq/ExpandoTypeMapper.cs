using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Expressions;

namespace ALE.ETLBox.DynamicLinq;

/// <summary>
/// Maps an <see cref="ExpandoObject"/> to a runtime DynamicClass instance with typed
/// properties so that <c>System.Linq.Dynamic.Core</c> can resolve property names
/// against a real CLR type.
/// </summary>
/// <remarks>
/// <para>
/// Has two execution paths chosen automatically per row:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>Fast path</b> for flat shapes (scalars, nullables, strings, byte arrays, custom
/// classes). Compiles a per-shape <c>Func&lt;IDictionary, object&gt;</c> via
/// Expression Trees once per shape and caches it; per-row cost is one dict walk +
/// cache lookup + delegate invoke. No reflection in the hot path. Covers the typical
/// Common.Etl XML flow (DB row -> ExpandoObject with scalar fields).
/// </description></item>
/// <item><description>
/// <b>Slow path</b> for shapes with nested <see cref="IDictionary{TKey,TValue}"/> or
/// homogeneous collections (lists of dictionaries or scalars). Recurses into nested
/// shapes and uses reflection-based property assignment. Same behaviour as before
/// optimization - kept for compatibility with predicates over nested objects
/// (<c>Order.Total > 100</c>, <c>Items.Any(Sum > 100)</c>, etc.).
/// </description></item>
/// </list>
/// <para>
/// The fast-path cache keys on <c>(field name, runtime value type)</c> tuples. A row
/// with <c>Reserve = null</c> produces a different signature than one with
/// <c>Reserve = 100m</c> (signature for null rows uses <c>typeof(object)</c>,
/// signature for valued rows uses the concrete type) - the cache holds two compiled
/// mappers, one per variant.
/// </para>
/// <para>
/// <c>string</c> and <c>byte[]</c> are deliberately treated as scalar values rather
/// than as <c>IEnumerable</c> collections. Heterogeneous collections (items with
/// different field sets or types) throw <see cref="InvalidOperationException"/> from
/// the slow path.
/// </para>
/// <para>
/// Internal API - used by <see cref="ExpressionRowFiltration"/>. Not part of the
/// public surface.
/// </para>
/// </remarks>
internal static class ExpandoTypeMapper
{
    private static readonly ConcurrentDictionary<ShapeSignature, ShapeEntry> _fastPathCache = new();

    /// <summary>
    /// Maps the row to a runtime DynamicClass instance. Routes to the fast compiled
    /// mapper for flat shapes or the recursive reflection mapper for shapes that
    /// contain nested dictionaries or collections.
    /// </summary>
    public static (Type Type, object Instance) Map(ExpandoObject row)
    {
        var dict = (IDictionary<string, object?>)row;

        // One walk: build signature and detect complex fields together. If the row
        // contains a nested IDictionary or non-string/non-byte-array enumerable,
        // ShapeSignature.TryFrom returns false and we route to the slow path.
        if (!ShapeSignature.TryFrom(dict, out var signature))
        {
            return MapWithReflection(dict);
        }

        var entry = _fastPathCache.GetOrAdd(signature, BuildCompiledEntry);
        var instance = entry.Mapper(dict);
        return (entry.Type, instance);
    }

    // === Fast path: compiled per-shape mapper ===

    // Builds a compiled mapper once per unique shape signature. Subject to
    // ConcurrentDictionary.GetOrAdd factory race semantics - multiple invocations
    // under contention are functionally equivalent (same expression, same shape).
    private static ShapeEntry BuildCompiledEntry(ShapeSignature signature)
    {
        var properties = new DynamicProperty[signature.Fields.Count];
        for (var i = 0; i < signature.Fields.Count; i++)
        {
            properties[i] = new DynamicProperty(signature.Fields[i].Name, signature.Fields[i].Type);
        }
        var type = DynamicClassFactory.CreateType(properties);

        // Compile lambda body equivalent to:
        //   dict => new T { F1 = (T1)dict["F1"], F2 = (T2)dict["F2"], ... }
        var dictParam = Expression.Parameter(typeof(IDictionary<string, object?>), "dict");
        var indexer =
            typeof(IDictionary<string, object?>).GetProperty("Item")
            ?? throw new InvalidOperationException(
                "IDictionary<string, object?>.Item indexer not found."
            );

        var bindings = new MemberBinding[signature.Fields.Count];
        for (var i = 0; i < signature.Fields.Count; i++)
        {
            var field = signature.Fields[i];
            var indexAccess = Expression.MakeIndex(
                dictParam,
                indexer,
                new Expression[] { Expression.Constant(field.Name) }
            );

            // Value types - explicit Convert; the compiled mapper for this shape is
            // only used for rows whose value at this position is non-null (signature
            // would have been (object) instead, mapped to a different cache entry).
            // Reference types - TypeAs preserves null safely.
            Expression typedValue = field.Type.IsValueType
                ? (Expression)Expression.Convert(indexAccess, field.Type)
                : Expression.TypeAs(indexAccess, field.Type);

            bindings[i] = Expression.Bind(type.GetProperty(field.Name)!, typedValue);
        }

        var body = Expression.MemberInit(Expression.New(type), bindings);
        var lambda = Expression.Lambda<Func<IDictionary<string, object?>, object>>(
            Expression.Convert(body, typeof(object)),
            dictParam
        );

        return new ShapeEntry(type, lambda.Compile());
    }

    // === Slow path: recursive reflection-based mapper ===

    // Original mapping logic preserved for shapes the compiled fast path cannot
    // express directly (nested dictionaries, homogeneous collections). Cost is one
    // walk to build properties + Activator.CreateInstance + reflection-based field
    // assignment; recursion handles arbitrary nesting depth.
    private static (Type Type, object Instance) MapWithReflection(IDictionary<string, object?> dict)
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
        if (raw is IDictionary<string, object?> nestedDict)
        {
            var (nestedType, nestedInstance) = MapWithReflection(nestedDict);
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

        if (first is IDictionary<string, object?> firstDict)
        {
            elementType = MapWithReflection(firstDict).Type;
            projectItem = item =>
            {
                if (item is not IDictionary<string, object?> d)
                    throw new InvalidOperationException(
                        "Heterogeneous collection: mix of dictionary and non-dictionary items."
                    );
                var (itemType, itemInstance) = MapWithReflection(d);
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

    // === Shape signature for the fast-path cache ===

    private sealed class ShapeEntry
    {
        public Type Type { get; }
        public Func<IDictionary<string, object?>, object> Mapper { get; }

        public ShapeEntry(Type type, Func<IDictionary<string, object?>, object> mapper)
        {
            Type = type;
            Mapper = mapper;
        }
    }

    private readonly struct ShapeSignature : IEquatable<ShapeSignature>
    {
        private readonly FieldKey[] _fields;
        private readonly int _hash;

        public IReadOnlyList<FieldKey> Fields => _fields;

        private ShapeSignature(FieldKey[] fields, int hash)
        {
            _fields = fields;
            _hash = hash;
        }

        // Single-walk entry: builds the signature for flat shapes, or signals a
        // complex shape (nested IDictionary or collection field) by returning false.
        // Caller routes complex shapes to the reflection-based slow path.
        public static bool TryFrom(IDictionary<string, object?> dict, out ShapeSignature signature)
        {
            var fields = new FieldKey[dict.Count];
            var hash = 17;
            var i = 0;
            foreach (var pair in dict)
            {
                if (pair.Value is IDictionary<string, object> || IsCollection(pair.Value))
                {
                    signature = default;
                    return false;
                }

                var t = pair.Value?.GetType() ?? typeof(object);
                fields[i] = new FieldKey(pair.Key, t);
                hash = unchecked(hash * 31 + pair.Key.GetHashCode());
                hash = unchecked(hash * 31 + t.GetHashCode());
                i++;
            }
            signature = new ShapeSignature(fields, hash);
            return true;
        }

        // Treat string and byte[] as scalars (their content is opaque to predicates).
        private static bool IsCollection(object? value) =>
            value is not null and not string and not byte[] and IEnumerable;

        public bool Equals(ShapeSignature other)
        {
            if (_hash != other._hash)
                return false;
            if (_fields.Length != other._fields.Length)
                return false;
            for (var i = 0; i < _fields.Length; i++)
            {
                if (!_fields[i].Equals(other._fields[i]))
                    return false;
            }
            return true;
        }

        public override bool Equals(object? obj) => obj is ShapeSignature other && Equals(other);

        public override int GetHashCode() => _hash;

        public readonly struct FieldKey : IEquatable<FieldKey>
        {
            public string Name { get; }
            public Type Type { get; }

            public FieldKey(string name, Type type)
            {
                Name = name;
                Type = type;
            }

            public bool Equals(FieldKey other) => Name == other.Name && Type == other.Type;

            public override bool Equals(object? obj) => obj is FieldKey other && Equals(other);

            public override int GetHashCode()
            {
                unchecked
                {
                    var h = 17;
                    h = h * 31 + Name.GetHashCode();
                    h = h * 31 + Type.GetHashCode();
                    return h;
                }
            }
        }
    }
}
