using System.Xml;

namespace ALE.ETLBox.Serialization.DataFlow
{
    public static class DataFlowSerializationExtensions
    {
        public static void ReadFromXml(this IDataFlow dataFlow, XmlReader reader)
        {
            var xmlReader = new DataFlowXmlReader(dataFlow);
            xmlReader.Read(reader);
        }
    }
}
