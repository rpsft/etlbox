using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using ALE.ETLBox.DataFlow;
using ETLBox.Primitives;

namespace ALE.ETLBox.Serialization.DataFlow;

/// <summary>
/// Read data flow configuration from XML
/// </summary>
public sealed class DataFlowXmlReader
{
    // Type cache for all serializable Dataflow types
    private readonly Type[] _types;

    // Data flow instance to configure
    private readonly IDataFlow _dataFlow;

    // Universal error handler for all supporting sources
    private readonly IDataFlowDestination<ETLBoxError>? _linkAllErrorsTo;

    // Check if the universal error destination was added to data flow
    private bool _allErrorsDestinationAdded;

    public DataFlowXmlReader(
        IDataFlow dataFlow,
        IDataFlowDestination<ETLBoxError>? linkAllErrorsTo = null
    )
    {
        _linkAllErrorsTo = linkAllErrorsTo;
        _dataFlow = dataFlow ?? throw new ArgumentNullException(nameof(dataFlow));
        _dataFlow.Destinations = new List<IDataFlowDestination<ExpandoObject>>();
        _dataFlow.ErrorDestinations = new List<IDataFlowDestination<ETLBoxError>>();

        var types = new List<Type>();
        var assemblies = AppDomain.CurrentDomain.GetAssemblies().ToList();
        var files = Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory, "*.dll");

        foreach (var file in files)
        {
            try
            {
                var name = AssemblyName.GetAssemblyName(file);
                if (assemblies.Find(a => a.FullName == name.FullName) is not null)
                {
                    continue;
                }

                var assembly = Assembly.Load(name);
                assemblies.Add(assembly);
            }
            catch (BadImageFormatException)
            {
                // Ignore
            }
        }

        foreach (var currentTypes in assemblies.Select(GetDataFlowTypes))
        {
            types.AddRange(currentTypes);
        }

        _types = types.ToArray();
    }

    public void Read(XmlReader reader)
    {
        reader.MoveToContent();
        var root = reader.Name;
        var namespaceUri = reader.NamespaceURI;
        reader.Read();
        reader.MoveToContent();
        while (!reader.EOF)
        {
            while (reader.NodeType == XmlNodeType.Whitespace)
                reader.Skip();
            if (reader.NodeType == XmlNodeType.None)
                reader.Skip();
            if (
                reader.NodeType == XmlNodeType.EndElement
                && reader.LocalName == root
                && reader.NamespaceURI == namespaceUri
            )
            {
                break;
            }
            InitializeRootPropertiesFromXml(reader);
        }
    }

    public static T Deserialize<T>(string xml, ErrorLogDestination errorLogDestination)
        where T : IDataFlow, new()
    {
        using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
        using var xmlReader = XmlReader.Create(stream);
        var step = Activator.CreateInstance<T>();
        var reader = new DataFlowXmlReader(step, errorLogDestination);
        reader.Read(xmlReader);
        return step;
    }

    private void InitializeRootPropertiesFromXml(XmlReader reader)
    {
        var prop = _dataFlow.GetType().GetProperties().LastOrDefault(p => p.Name == reader.Name);

        var type = GetPropertyType(prop, reader.Name);

        if (IsSourceType(type))
        {
            _dataFlow.Source =
                CreateSource(type, reader)
                ?? throw new InvalidOperationException(
                    $"Invalid configuration. Root source '{reader.Name}' must implement {nameof(IDataFlowSource<ExpandoObject>)}"
                );
            return;
        }

        if (prop is null)
        {
            throw new InvalidDataException(
                $"Invalid configuration. Property '{reader.Name}' not found"
            );
        }

        var element = (XElement)XNode.ReadFrom(reader);

        if (IsValueTypeProperty(prop))
        {
            SetValueTypeProperty(_dataFlow, prop, element);
            return;
        }

        if (!prop.PropertyType.IsClass)
        {
            throw new InvalidDataException(
                $"Invalid configuration. Property '{reader.Name}' deserialization is not implemented"
            );
        }

        SetClassProperty(_dataFlow, prop, element);
    }

    private void SetClassProperty(object instance, PropertyInfo prop, XElement? element)
    {
        if (element is null)
        {
            throw new InvalidDataException(
                $"Invalid configuration. Property '{prop.Name}' xml element is null"
            );
        }

        var value = CreateObject(prop.PropertyType, element);

        if (value is null)
        {
            return;
        }

        prop.SetValue(instance, value);
    }

    private static void SetValueTypeProperty(object instance, PropertyInfo prop, XElement? element)
    {
        if (element is null)
        {
            throw new InvalidDataException(
                $"Invalid configuration. Property '{prop.Name}' xml element is null"
            );
        }

        var value = GetValue(prop.PropertyType, element.Value);

        if (value is null)
        {
            return;
        }

        prop.SetValue(instance, value);
    }

    private static bool IsValueTypeProperty(PropertyInfo prop)
    {
        return IsValueType(prop.PropertyType);
    }

    private static bool IsSourceType(Type type)
    {
        var interfaces = type.GetInterfaces();

        return Array.Exists(
            interfaces,
            i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDataFlowSource<>)
        );
    }

    private Type GetPropertyType(PropertyInfo? prop, string typeName)
    {
        return prop?.PropertyType ?? GetTypeByName(_types, typeName);
    }

    private IDataFlowSource<ExpandoObject>? CreateSource(Type type, XmlReader reader)
    {
        var element = (XElement)XNode.ReadFrom(reader);

        return CreateObject(type, element) as IDataFlowSource<ExpandoObject>;
    }

    private object? CreateObject(Type type, XContainer node)
    {
        if (IsValueType(type))
        {
            return CreateValueType(type, node);
        }

        if (type.IsArray)
        {
            return CreateArray(type, node);
        }

        if (
            Array.Exists(
                type.GetInterfaces(),
                i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IList<>)
            )
        )
        {
            return CreateList(type, node);
        }

        return IsDictionary(type) ? CreateDictionary(type, node) : CreateInstance(type, node);
    }

    private object? CreateList(Type type, XContainer node)
    {
        var elementType = type.GenericTypeArguments[0]
                          ?? throw new InvalidDataException(
                              $"Invalid configuration. Implementation for element type of array '{type}' not found"
                              );

        var list = DataFlowActivator.CreateInstance(type);
        var set = type.GetMethod("Add");
        if (set == null)
            return list;
        foreach (var t in node.Elements())
        {
            var item = CreateObject(elementType, t);
            if (item != null)
            {
                set.Invoke(list, new[] { item });
            }
        }
        return list;
    }

    private object? CreateInstance(Type type, XContainer node)
    {
        var instance = DataFlowActivator.CreateInstance(type);

        if (instance is null)
        {
            return null;
        }

        foreach (var propXml in node.Elements())
        {
            InitializeInstanceProperty(instance, type, propXml);
        }

        if (_linkAllErrorsTo == null || instance is not ILinkErrorSource errorSource)
        {
            return instance;
        }

        errorSource.LinkErrorTo(_linkAllErrorsTo);
        if (_allErrorsDestinationAdded)
        {
            return instance;
        }

        _dataFlow.ErrorDestinations.Add(_linkAllErrorsTo);
        _allErrorsDestinationAdded = true;

        return instance;
    }

    private void InitializeInstanceProperty(object instance, MemberInfo type, XElement? propXml)
    {
        if (propXml is null)
        {
            throw new InvalidDataException($"Cant get a property for type '{type}'");
        }

        var prop = instance.GetType().GetProperty(propXml.Name.LocalName);
        if (prop is null || !prop.CanWrite)
        {
            TryInvokeSourceMethod(instance, propXml);
            return;
        }

        if (prop.PropertyType.IsEnum)
        {
            SetEnumProperty(instance, prop, propXml);
            return;
        }

        if (IsNullableEnum(prop.PropertyType))
        {
            SetNullableEnumProperty(instance, prop, propXml);
            return;
        }

        if (IsValueType(prop.PropertyType))
        {
            SetValueTypeProperty(instance, prop, propXml);
            return;
        }

        if (prop.PropertyType.IsInterface || prop.PropertyType.IsAbstract)
        {
            SetInterfaceProperty(instance, type.Name, propXml, prop);
            return;
        }

        if (prop.PropertyType.IsClass)
        {
            SetClassProperty(instance, prop, propXml);
        }
    }

    private static bool IsNullableEnum(Type t)
    {
        var underlyingType = Nullable.GetUnderlyingType(t);
        return underlyingType is { IsEnum: true };
    }

    private static void SetEnumProperty(object instance, PropertyInfo prop, XElement? element)
    {
        if (element is null)
        {
            throw new InvalidDataException(
                $"Invalid configuration. Property '{prop.Name}' xml element is null"
            );
        }

        var eval = Enum.Parse(prop.PropertyType, element.Value);
        prop.SetValue(instance, eval);
    }

    private static void SetNullableEnumProperty(
        object instance,
        PropertyInfo prop,
        XElement? element
    )
    {
        if (string.IsNullOrEmpty(element?.Value))
        {
            if (prop.PropertyType.IsNullable())
            {
                prop.SetValue(instance, null);
            }

            return;
        }

        var underlyingType =
            Nullable.GetUnderlyingType(prop.PropertyType) ?? throw new InvalidOperationException();
        var eval = Enum.Parse(underlyingType, element!.Value);
        prop.SetValue(instance, eval);
    }

    private void SetInterfaceProperty(
        object instance,
        string typeName,
        XElement propXml,
        PropertyInfo prop
    )
    {
        Type? propType;
        // If Type is IEnumerable, assign array
        if (
            prop.PropertyType.IsGenericType
            && prop.PropertyType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
        )
        {
            propType = prop.PropertyType.GenericTypeArguments[0].MakeArrayType();
        }
        else
        {
            var propTypeName = propXml.Attribute("type")?.Value;
            if (string.IsNullOrEmpty(propTypeName))
            {
                throw new InvalidDataException(
                    $"Type attribute is required to assign '{typeName}.{prop.Name}' value"
                );
            }

            propType = GetType(propTypeName!, prop.PropertyType);

            if (propType is null)
            {
                throw new InvalidDataException(
                    $"Type '{propType}' is not found for '{typeName}.{prop.Name}'"
                );
            }
        }

        var value = CreateObject(propType, propXml);
        if (value is null)
        {
            return;
        }

        prop.SetValue(instance, value);
    }

    private Array CreateArray(Type type, XContainer node)
    {
        var elementType = type.GetElementType() ?? throw new InvalidDataException(
                $"Invalid configuration. Implementation for element type of array '{type}' not found"
            );

        var elements = node.Elements().ToArray();
        var array = Array.CreateInstance(elementType, elements.Length);
        var set = type.GetMethod("SetValue", new[] { typeof(object), typeof(int) });
        for (var i = 0; i < elements.Length; i++)
        {
            var item = CreateObject(elementType, elements[i]);
            if (item != null)
            {
                set?.Invoke(array, new[] { item, i });
            }
        }

        return array;
    }

    private static object? CreateValueType(Type type, XContainer node)
    {
        return node.FirstNode is null ? null : GetValue(type, node.FirstNode.ToString().Trim());
    }

    private object CreateDictionary(Type type, XContainer node)
    {
        var arguments = type.GetGenericArguments();

        if (arguments is null || arguments.Length < 2)
        {
            throw new InvalidDataException(
                $"Invalid configuration. Correct implementation of dictionary type '{type}' not found"
            );
        }

        var keyType = arguments[0];
        var valueType = arguments[1];

        if (keyType is null || keyType != typeof(string))
        {
            throw new InvalidDataException(
                $"Invalid configuration. Key type of dictionary '{type}' should be string"
            );
        }

        if (valueType is null)
        {
            throw new InvalidDataException(
                $"Invalid configuration. Implementation for key value of dictionary '{type}' not found"
            );
        }

        var dictToCreate = typeof(Dictionary<,>).MakeGenericType(arguments);
        var dict = Activator.CreateInstance(dictToCreate) ?? throw new InvalidOperationException(
                $"Invalid configuration. Can't create dictionary of type '{type}'"
            );

        var elements = node.Elements().ToArray();
        var add = type.GetMethod("Add", new[] { keyType, valueType });

        foreach (var element in elements)
        {
            var item = CreateObject(valueType, element);
            if (item != null)
            {
                add?.Invoke(dict, new[] { element.Name.LocalName, item });
            }
        }

        return dict;
    }

    private void TryInvokeSourceMethod(object instance, XElement propXml)
    {
        var method = GetMethod(instance, propXml);

        if (method is null)
        {
            return;
        }

        using var reader = propXml.CreateReader();
        reader.MoveToContent();
        var root = (XElement)XNode.ReadFrom(reader);
        foreach (var element in root.Elements())
        {
            AddDestinationAndInvokeMethod(element, method, instance);
        }
    }

    private void AddDestinationAndInvokeMethod(XElement element, MethodBase method, object source)
    {
        // This should be LinkTo, LinkErrorTo, or similar linking method with single IDataFlowLinkTarget<> parameter
        if (
            method.GetParameters().Length != 1
            || !method.GetParameters()[0].ParameterType.IsInterface
            || !typeof(IDataFlowLinkTarget<>).IsAssignableFrom(
                method.GetParameters()[0].ParameterType.GetGenericTypeDefinition()
            )
        )
            return;

        var parameterType = method.GetParameters()[0].ParameterType;
        var type = GetTypeByName(_types, element.Name.LocalName);

        if (!parameterType.IsAssignableFrom(type))
        {
            throw new InvalidOperationException(
                $"Type '{type.Name}' is not assignable to parameter type '{parameterType.Name}' in method '{method.Name}'"
            );
        }

        var obj = CreateObject(type, element);

        if (obj is IDataFlowDestination<ExpandoObject> dest)
        {
            _dataFlow.Destinations.Add(dest);
        }
        if (obj is IDataFlowDestination<ETLBoxError> err)
        {
            _dataFlow.ErrorDestinations.Add(err);
        }

        // Call LinkTo or LinkErrorTo method
        method.Invoke(source, new[] { obj });
    }

    private static MethodInfo? GetMethod(object instance, XElement propXml)
    {
        return instance
            .GetType()
            .GetMethods()
            .Where(m => m.Name == propXml.Name.LocalName)
            .OrderBy(m => m.GetParameters().Length)
            .FirstOrDefault();
    }

    private static Type? GetType(string typeName, Type? baseType = null)
    {
        var isArray = false;
        if (typeName.EndsWith("[]"))
        {
            isArray = true;
            typeName = typeName.Remove(typeName.Length - 2, 2);
        }

        Type? type = null;
        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            var types = assembly.GetTypes().Where(t => t.Name == typeName);
            if (baseType is { IsInterface: true })
            {
                types = types.Where(t => Array.Exists(t.GetInterfaces(), i => i == baseType));
            }
            type = types.LastOrDefault(t => t.Name == typeName);
            if (type != null)
            {
                break;
            }
        }

        return isArray ? type?.MakeArrayType() : type;
    }

    private static object? GetValue(Type type, string value)
    {
        value = value.Trim();

        if (type.IsNullable() && string.IsNullOrEmpty(value))
        {
            return null;
        }

        try
        {
            if (value.TryParse(type, out var objValue))
            {
                return objValue;
            }
        }
        catch
        {
            throw new InvalidOperationException(
                $"Invalid configuration. Value '{value}' for type '{type}' is not valid"
            );
        }

        return value;
    }

    private static bool IsDictionary(Type type)
    {
        return type.IsGenericType
            && Array.Exists(
                type.GetInterfaces(),
                i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
            );
    }

    private static bool IsValueType(Type type)
    {
        return type.IsValueType || type == typeof(string);
    }

    private static Type GetTypeByName(Type[] types, string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new InvalidDataException("Invalid configuration. Type name is empty");
        }
        // First look for non-generic types, exactly matching the name
        var type = Array.Find(types, t => t.Name == name && !t.IsGenericTypeDefinition);
        if (type != null)
        {
            return type;
        }
        // Now look for generic types, matching the name, accepting single type parameter
        type = Array.Find(
            types,
            t =>
                t.Name.StartsWith($"{name}`")
                && t.IsGenericTypeDefinition
                && t.GetGenericArguments().Length == 1
        );
        if (type is null)
        {
            throw new InvalidOperationException($"Could not find type by name '{name}'");
        }
        return type.MakeGenericType(typeof(ExpandoObject));
    }

    private static IEnumerable<Type> GetDataFlowTypes(Assembly assembly)
    {
        try
        {
            return assembly
                .GetTypes()
                .Where(t =>
                    t.IsClass
                    && Array.Exists(
                        t.GetInterfaces(),
                        i =>
                            i.IsGenericType
                            && (
                                i.GetGenericTypeDefinition().FullName
                                    == typeof(IDataFlowLinkSource<>).FullName
                                || i.GetGenericTypeDefinition().FullName
                                    == typeof(IDataFlowLinkTarget<>).FullName
                                || i.GetGenericTypeDefinition().FullName
                                    == typeof(IDataFlowTransformation<,>).FullName
                            )
                    )
                );
        }
        catch
        {
            // Could not read types - continue with next assembly
            return Enumerable.Empty<Type>();
        }
    }
}
