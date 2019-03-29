using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class TypeInfo
    {
        internal PropertyInfo[] Properties { get; set; }
        internal List<string> PropertyNames { get; set; }
        internal Dictionary<string, int> PropertyIndex { get; set; }
        internal int PropertyLength { get; set; }
        internal bool IsArray { get; set; } = true;

        internal TypeInfo(Type typ)
        {
            PropertyNames = new List<string>();
            PropertyIndex = new Dictionary<string, int>();
            GatherTypeInfos(typ);
        }
        private void GatherTypeInfos(Type typ)
        {
            IsArray = typ.IsArray;
            if (!typ.IsArray)
            {
                Properties = typ.GetProperties();
                PropertyLength = Properties.Length;
                int index = 0;
                foreach (var propInfo in Properties)
                {
                    PropertyNames.Add(propInfo.Name);
                    PropertyIndex.Add(propInfo.Name, index++);
                }
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
        internal PropertyInfo GetProperty(string name) => Properties[PropertyIndex[name]];
    }
}

