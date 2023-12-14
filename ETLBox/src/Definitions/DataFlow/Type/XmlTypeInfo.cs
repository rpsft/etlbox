using System.Xml.Serialization;

namespace ALE.ETLBox.src.Definitions.DataFlow.Type
{
    internal sealed class XmlTypeInfo : TypeInfo
    {
        internal string ElementName { get; }

        internal XmlTypeInfo(System.Type type)
            : base(type)
        {
            GatherTypeInfo();
            foreach (Attribute customAttribute in Attribute.GetCustomAttributes(type))
            {
                if (customAttribute is XmlRootAttribute attribute)
                {
                    ElementName = attribute.ElementName;
                }
            }
            if (string.IsNullOrWhiteSpace(ElementName))
                ElementName = type.Name;
        }
    }
}
