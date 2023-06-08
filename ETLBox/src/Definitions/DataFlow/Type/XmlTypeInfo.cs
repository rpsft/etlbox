﻿using System.Xml.Serialization;

namespace ALE.ETLBox.DataFlow
{
    internal class XmlTypeInfo : TypeInfo
    {
        internal string ElementName { get; set; }

        internal XmlTypeInfo(Type typ)
            : base(typ)
        {
            GatherTypeInfo();
            foreach (Attribute attr in Attribute.GetCustomAttributes(typ))
            {
                if (attr is XmlRootAttribute)
                {
                    var a = attr as XmlRootAttribute;
                    ElementName = a.ElementName;
                }
            }
            if (string.IsNullOrWhiteSpace(ElementName))
                ElementName = typ.Name;
        }
    }
}
