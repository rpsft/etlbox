using System.Dynamic;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;
using System.Xml.Serialization;
using ALE.ETLBox.DataFlow;
using CsvHelper.Configuration;
using ETLBox.Primitives;
using FluentAssertions;

namespace TestSerialization
{
    public class SerializationTests
    {
        [Fact]
        public void Test()
        {
            var xml = @"<Step>
                <JsonSource>
                    <Uri>C:/Temp/1.json</Uri>
                    <LinkTo>
                        <JsonTransformation>
                            <Mappings>        
                                <Mapping>
                                    <Source>
                                        <Name>Id</Name>
                                        <Path>$.Data.Id</Path>
                                    </Source>
                                    <Destination>Col1</Destination>
                                </Mapping>
                            </Mappings>        
                            <LinkTo>
                                <CsvDestination>
                                    <Uri>C:/Temp/1.csv</Uri>
                                </CsvDestination>
                                <DbDestination>
                                    <TableName>dbo.TestTable</TableName>
                                    <ConnectionManager type=""PostgresConnectionManager"">
                                        <ConnectionString type=""PostgresConnectionString"">
                                            <Value>host=.;Port=5432;Database=TestDb;User ID=user;Password=123</Value>
                                        </ConnectionString>
                                    </ConnectionManager>
                                </DbDestination>
                            </LinkTo>
                        </JsonTransformation>
                    </LinkTo>
                </JsonSource>
            </Step>";

            using var stream = new MemoryStream(Encoding.Default.GetBytes(xml));
            var serializer = new XmlSerializer(typeof(Step));
            var step = (Step)serializer.Deserialize(stream)!;

            step.Should().NotBeNull();
            step!.Source.Should().BeOfType<JsonSource<ExpandoObject>>();
            ((JsonSource<ExpandoObject>)step.Source).Uri.Should().Be("C:/Temp/1.json");
        }
    }

    public class Step : IXmlSerializable
    {
        private readonly Type[] _types;

        private List<IDataFlowDestination<ExpandoObject>> _destinations = new();

        public Step()
        {
            var types = new List<Type>();
            var files = Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory, "*.dll");
            foreach (var file in files)
            {
                var assembly = Assembly.LoadFrom(file);

                var tr = assembly.GetTypes().Where(t => t.IsClass && t.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDataFlowTransformation<,>)));

                var currentTypes = assembly.GetTypes().Where(t => t.IsClass && t.GetInterfaces()
                            .Any(i => i.IsGenericType && (
                                    i.GetGenericTypeDefinition() == typeof(IDataFlowLinkSource<>)
                                 || i.GetGenericTypeDefinition() == typeof(IDataFlowLinkTarget<>)
                                 || i.GetGenericTypeDefinition() == typeof(IDataFlowTransformation<,>)
                             )));
                types.AddRange(currentTypes);
            }

            _types = types.ToArray();
        }

        public IDataFlowSource<ExpandoObject> Source { get; set; } = null!;

        public XmlSchema? GetSchema() => null;

        public virtual void ReadXml(XmlReader reader)
        {
            reader.MoveToContent();
            while (reader.Read())
            {
                var type = _types.FirstOrDefault(t => t.Name.StartsWith(reader.Name));
                if (type == null)
                {
                    continue;
                }
                Source = CreateSource(type, reader) ?? throw new InvalidOperationException($"Invalid configuration. Root source '{type}' must implement {nameof(IDataFlowSource<ExpandoObject>)}");
            }
        }

        public void WriteXml(XmlWriter writer)
        {
            throw new NotImplementedException();
        }



        private IDataFlowSource<ExpandoObject>? CreateSource(Type type, XmlReader reader)
        {
            var element = (XElement)XElement.ReadFrom(reader);

            return CreateObject(type, element) as IDataFlowSource<ExpandoObject>;
        }

        private object CreateObject(Type type, XElement element)
        {
            if (type.IsArray)
            {
                var elementType = type.GetElementType();
                var elements = element.Elements().ToArray();
                var array = Array.CreateInstance(elementType!, elements.Length);
                var set = type.GetMethod("SetValue", new[] { typeof(object), typeof(int) });
                for (int i=0; i < elements.Length; i++)
                {
                    var item = CreateObject(elementType, elements[i]);
                    set?.Invoke(array, new object[] { item, i });
                }
                return array;
            }

            var instance = CreateInstance(type);

            foreach (var propXml in element.Elements())
            {
                var prop = instance!.GetType().GetProperty(propXml.Name.LocalName);
                if (prop == null || !prop.CanWrite)
                {
                    var method = instance!.GetType().GetMethods()
                        .Where(m => m.Name == propXml.Name.LocalName)
                        .OrderBy(m => m.GetParameters().Length)
                        .FirstOrDefault();
                    if (method != null)
                    {
                        using var reader = propXml.CreateReader();
                        var source = instance as IDataFlowLinkSource<ExpandoObject>;
                        if (source == null)
                        {
                            throw new InvalidOperationException($"Configuration error, Type '{instance.GetType().Name}' is not implemented {nameof(IDataFlowLinkSource<ExpandoObject>)}");
                        }
                        InvokeMethod(method, source, reader);
                    }
                    continue;
                }
                if (prop.PropertyType.IsValueType || prop.PropertyType == typeof(string))
                {
                    prop.SetValue(instance, propXml.Value);
                }
                else
                if (prop.PropertyType.IsInterface || prop.PropertyType.IsAbstract)
                {
                    var typeName = propXml.Attribute("type")?.Value;
                    if (string.IsNullOrEmpty(typeName))
                    {
                        throw new InvalidDataException($"Не задан атрибут типа для свойства '{type.Name}.{prop.Name}'");
                    }
                    var propType = GetType(typeName);

                    if (propType == null)
                    {
                        throw new InvalidDataException($"Не удалось получить тип для свойства '{type.Name}.{prop.Name}'");
                    }
                    var value = CreateObject(propType, propXml);
                    prop.SetValue(instance, value);
                }
                else
                if (prop.PropertyType.IsClass)
                {
                    var value = CreateObject(prop.PropertyType, propXml);
                    prop.SetValue(instance, value);
                }
            }
            return instance!;
        }

        private void InvokeMethod(MethodInfo method, IDataFlowLinkSource<ExpandoObject>? source, XmlReader reader)
        {
            reader.MoveToContent();
            XElement root = (XElement)XElement.ReadFrom(reader);
            foreach (var element in root.Elements())
            {
                var type = _types.LastOrDefault(t => t.Name.StartsWith(element.Name.LocalName));
                if (type != null)
                {
                    var target = CreateObject(type!, element) as IDataFlowLinkTarget<ExpandoObject>;
                    if (target == null)
                    {
                        // logging
                        continue;
                    }
                    if (method.GetParameters().Length == 1
                     && method.GetParameters()[0].ParameterType.IsInterface
                     && method.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(IDataFlowLinkTarget<>))
                    {
                        if (target is IDataFlowDestination<ExpandoObject> dest)
                        {
                            _destinations.Add(dest);
                        }
                        method?.Invoke(source, new[] { target });
                    }
                }
                else
                {
                    // logging
                }
            }
        }

        private static object? CreateInstance(Type type)
        {
            var method = Extensions.GetMethod(type);
            if (method != null)
            {
                return method.Invoke(null, null);
            }

            Type constructedType = type;
            if (type.IsGenericType)
            {
                constructedType = type.MakeGenericType(typeof(ExpandoObject));
            }
            var instance = Activator.CreateInstance(constructedType);
            return instance;
        }

        private static Type? GetType(string typeName)
            => AppDomain.CurrentDomain
                .GetAssemblies()
                .SelectMany(x => x.GetTypes())
                .FirstOrDefault(t => t.Name == typeName);
    }

    // Класс вместе с шагом нужно перенести в RapidSoft.Etl
    public static class Extensions
    {
        public static CsvConfiguration Create()
            => new CsvConfiguration(CultureInfo.InvariantCulture);

        // Находим метод, возвращающий экземпляр требуемого типа
        public static MethodInfo GetMethod(Type type)
        {
            var method = typeof(Extensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .FirstOrDefault(m => m.ReturnParameter.ParameterType == type);
            return method;
        }
    }
}
