using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class TypeInfo
    {
        internal PropertyInfo[] PropertyInfos { get; set; }
        internal List<string> PropertyNames { get; set; }
        internal int PropertyLength { get; set; }
        internal bool IsArray { get; set; } = true;

        internal TypeInfo(Type typ)
        {
            PropertyNames = new List<string>();
            GatherTypeInfos(typ);
        }
        private void GatherTypeInfos(Type typ)
        {
            IsArray = typ.IsArray;
            if (!typ.IsArray)
            {
                PropertyInfos = typ.GetProperties();
                PropertyLength = PropertyInfos.Length;
                foreach (var propInfo in PropertyInfos)
                    PropertyNames.Add(propInfo.Name);
            }

        }

        public static object CastPropertyValue(PropertyInfo property, string value)
        {
            if (property == null || String.IsNullOrEmpty(value))
                return null;
            if (property.PropertyType == typeof(bool))
                return value == "1" || value == "true" || value == "on" || value == "checked";
            else
                return Convert.ChangeType(value, property.PropertyType);
        }

        internal bool HasProperty(string name) => PropertyNames.Any(propName => propName == name);
    }
}

