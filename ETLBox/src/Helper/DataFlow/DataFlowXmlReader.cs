#nullable enable
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using ALE.ETLBox.DataFlow;

namespace ALE.ETLBox.Helper.DataFlow
{
    internal sealed class DataFlowXmlReader
    {
        private readonly Type[] _types;
        private readonly IDataFlow _dataFlow;

        public DataFlowXmlReader(IDataFlow dataFlow)
        {
            _dataFlow = dataFlow ?? throw new ArgumentNullException(nameof(dataFlow));
            _dataFlow.Destinations = new List<IDataFlowDestination<ExpandoObject>>();
            _dataFlow.ErrorDestinations = new List<IDataFlowDestination<ETLBoxError>>();

            var types = new List<Type>();
            var assemblies = AppDomain.CurrentDomain.GetAssemblies().ToList();
            var files = Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory, "*.dll");

            foreach (var file in files)
            {
                var name = AssemblyName.GetAssemblyName(file);
                if (assemblies.Find(a => a.FullName == name.FullName) is not null)
                {
                    continue;
                }

                var assembly = Assembly.Load(name);
                assemblies.Add(assembly);
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
            reader.Read();
            while (!(reader.Name == root && !reader.IsStartElement()))
            {
                InitializeDataFlowFromXml(reader);
            }
        }

        private void InitializeDataFlowFromXml(XmlReader reader)
        {
            var prop = _dataFlow.GetType().GetProperties().LastOrDefault(p => p.Name == reader.Name);

            var type = GetPropertyType(prop, reader.Name);

            if (IsSourceType(type))
            {
                _dataFlow.Source = CreateSource(type, reader)
                                   ?? throw new InvalidOperationException(
                                       $"Invalid configuration. Root source '{reader.Name}' must implement {nameof(IDataFlowSource<ExpandoObject>)}");
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
            return prop.PropertyType.IsValueType || prop.PropertyType == typeof(string);
        }

        private static bool IsSourceType(Type type)
        {
            var interfaces = type.GetInterfaces();

            if (interfaces is null)
            {
                return false;
            }

            return Array.Exists(interfaces, i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDataFlowSource<>));
        }

        private Type GetPropertyType(PropertyInfo? prop, string typeName)
        {
            var type = prop?.PropertyType;
            if (type is not null)
            {
                return type;
            }

            type = GetTypeByName(_types, typeName);

            if (type is null)
            {
                throw new InvalidDataException(
                    $"Invalid configuration. Implementation for property type '{typeName}' not found"
                );
            }

            return type;
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

            return IsDictionary(type) ? CreateDictionary(type, node) : CreateInstance(type, node);
        }

        private object? CreateInstance(Type type, XContainer node)
        {
            var instance = CreateInstance(type);

            if (instance is null)
            {
                return null;
            }

            foreach (var propXml in node.Elements())
            {
                InitializeInstanceProperty(instance, type, propXml);
            }

            if (instance is ILinkErrorSource linkErrorSource)
            {
                var errorLogDestination = new ErrorLogDestination();
                linkErrorSource.LinkErrorTo(errorLogDestination);
                _dataFlow.ErrorDestinations.Add(errorLogDestination);
            }

            return instance;
        }

        private void InitializeInstanceProperty(object instance, MemberInfo type, XElement? propXml)
        {
            if (propXml is null)
            {
                throw new InvalidDataException($"Не удалось получить свойство для типа '{type}'");
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

            if (prop.PropertyType.IsValueType || prop.PropertyType == typeof(string))
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

        private void SetInterfaceProperty(object instance, string typeName, XElement propXml, PropertyInfo prop)
        {
            var propTypeName = propXml.Attribute("type")?.Value;
            if (string.IsNullOrEmpty(propTypeName))
            {
                throw new InvalidDataException($"Не задан атрибут типа для свойства '{typeName}.{prop.Name}'");
            }

            var propType = GetType(propTypeName!);

            if (propType is null)
            {
                throw new InvalidDataException($"Не удалось получить тип для свойства '{typeName}.{prop.Name}'");
            }

            var value = CreateObject(propType, propXml);
            if (value is null)
            {
                return;
            }

            prop.SetValue(instance, value);
        }

        private Array? CreateArray(Type type, XContainer node)
        {
            var elementType = type.GetElementType();

            if (elementType is null)
            {
                throw new InvalidDataException(
                    $"Invalid configuration. Implementation for element type of array '{type}' not found"
                );
            }

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
            var dict = Activator.CreateInstance(dictToCreate);

            if (dict is null)
            {
                throw new InvalidOperationException(
                    $"Invalid configuration. Can't create dictionary of type '{type}'"
                );
            }

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

            if (instance is not IDataFlowLinkSource<ExpandoObject> source)
            {
                throw new InvalidDataException(
                    $"Configuration error, Type '{instance.GetType().Name}' is not implemented {nameof(IDataFlowLinkSource<ExpandoObject>)}");
            }

            using var reader = propXml.CreateReader();
            reader.MoveToContent();
            var root = (XElement)XNode.ReadFrom(reader);
            foreach (var element in root.Elements())
            {
                AddDestinationAndInvokeMethod(element, method, source);
            }
        }

        private void AddDestinationAndInvokeMethod(XElement element, MethodBase method, IDataFlowLinkSource<ExpandoObject> source)
        {
            var type = GetTypeByName(_types, element.Name.LocalName);

            if (type is null)
            {
                // logging
                return;
            }

            var obj = CreateObject(type, element);

            if (obj is not IDataFlowLinkTarget<ExpandoObject>)
            {
                // logging
                return;
            }

            if (method.GetParameters().Length != 1
                || !method.GetParameters()[0].ParameterType.IsInterface
                || method.GetParameters()[0].ParameterType.GetGenericTypeDefinition() !=
                typeof(IDataFlowLinkTarget<>))
                return;

            if (obj is IDataFlowDestination<ExpandoObject> dest)
            {
                _dataFlow.Destinations.Add(dest);
            }

            method.Invoke(source, new[] { obj });
        }

        private static MethodInfo? GetMethod(object instance, XElement propXml)
        {
            return instance.GetType().GetMethods()
                .Where(m => m.Name == propXml.Name.LocalName)
                .OrderBy(m => m.GetParameters().Length)
                .FirstOrDefault();
        }

        private static object? CreateInstance(Type type)
        {
            var method = DataFlowExtensions.GetMethod(type);
            if (method != null)
            {
                return method.Invoke(null, null);
            }

            var constructedType = type;
            if (type.IsGenericType)
            {
                constructedType = type.MakeGenericType(typeof(ExpandoObject));
            }

            return Activator.CreateInstance(constructedType);
        }

        private static Type? GetType(string typeName)
        {
            var isArray = false;
            if (typeName.EndsWith("[]"))
            {
                isArray = true;
                typeName = typeName.Remove(typeName.Length - 2, 2);
            }

            var type = AppDomain.CurrentDomain
                .GetAssemblies()
                .SelectMany(x => x.GetTypes())
                .FirstOrDefault(t => t.Name == typeName);

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
            return type.IsGenericType && Array.Exists(type.GetInterfaces(),
                i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)
            );
        }

        private static bool IsValueType(Type type)
        {
            return type.IsValueType || type == typeof(string);
        }

        private static Type? GetTypeByName(Type[] types, string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new InvalidDataException(
                    "Invalid configuration. Type name is empty"
                );
            }

            return Array.FindLast(types, t => t.Name.StartsWith($"{name}`"))
                   ?? Array.FindLast(types, t => t.Name == name);
        }

        private static IEnumerable<Type> GetDataFlowTypes(Assembly assembly)
        {
            return assembly.GetTypes().Where(t => t.IsClass && Array.Exists(t.GetInterfaces(),
                i => i.IsGenericType && (
                    i.GetGenericTypeDefinition().FullName == typeof(IDataFlowLinkSource<>).FullName
                    || i.GetGenericTypeDefinition().FullName == typeof(IDataFlowLinkTarget<>).FullName
                    || i.GetGenericTypeDefinition().FullName == typeof(IDataFlowTransformation<,>).FullName
                )));
        }
    }
}
