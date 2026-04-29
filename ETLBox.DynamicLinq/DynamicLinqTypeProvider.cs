using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core.CustomTypeProviders;
using System.Reflection;

namespace ALE.ETLBox.DynamicLinq;

/// <summary>
/// <see cref="IDynamicLinqCustomTypeProvider"/> implementation that exposes a known set
/// of types to the parser and applies optional namespace prefixes when resolving short
/// type names.
/// </summary>
/// <remarks>
/// Constructed by <see cref="ExpressionRowFiltration{TInput}"/> when any of
/// <see cref="ExpressionRowFiltration{TInput}.RegisterCustomTypes(Type[])"/>,
/// <see cref="ExpressionRowFiltration{TInput}.AdditionalAssemblyNames"/> or
/// <see cref="ExpressionRowFiltration{TInput}.AdditionalImports"/> is set. The
/// provider itself is stateless after construction. Internal API - not part of the
/// public surface.
/// </remarks>
internal sealed class DynamicLinqTypeProvider : IDynamicLinqCustomTypeProvider
{
    private readonly HashSet<Type> _types;
    private readonly string[] _imports;

    public DynamicLinqTypeProvider(HashSet<Type> types, string[]? imports = null)
    {
        _types = types;
        _imports = imports ?? Array.Empty<string>();
    }

    public HashSet<Type> GetCustomTypes() => _types;

    public Dictionary<Type, List<MethodInfo>> GetExtensionMethods() => new();

    public Type? ResolveType(string typeName)
    {
        // Direct match by full or short name.
        var direct = _types.FirstOrDefault(t => t.FullName == typeName || t.Name == typeName);
        if (direct is not null)
            return direct;

        // Try treating typeName as a short name within imported namespaces.
        return ResolveBySimpleNameInImports(typeName);
    }

    public Type? ResolveTypeBySimpleName(string simpleTypeName)
    {
        // Imported namespaces win over plain short-name match - explicit user
        // intent ("I imported this namespace, prefer types from it").
        return ResolveBySimpleNameInImports(simpleTypeName)
            ?? _types.FirstOrDefault(t => t.Name == simpleTypeName);
    }

    private Type? ResolveBySimpleNameInImports(string simpleName)
    {
        foreach (var import in _imports)
        {
            var qualified = $"{import}.{simpleName}";
            var match = _types.FirstOrDefault(t => t.FullName == qualified);
            if (match is not null)
                return match;
        }
        return null;
    }
}
