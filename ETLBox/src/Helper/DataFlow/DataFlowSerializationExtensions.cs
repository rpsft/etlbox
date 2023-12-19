using System.Xml;
using ETLBox.Primitives;

namespace ALE.ETLBox.Helper.DataFlow
{
    public static class DataFlowSerializationExtensions
    {
        public static void ReadFromXml(
            this IDataFlow dataFlow,
            XmlReader reader
        )
        {
            var xmlReader = new DataFlowXmlReader(dataFlow);
            xmlReader.Read(reader);
        }
    }
}
