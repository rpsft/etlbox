using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class DBTypeInfo : TypeInfo
    {
        internal List<string> PropertyNames { get; set; } = new List<string>();
        internal Dictionary<string, string> ColumnMap2Property { get; set; } = new Dictionary<string, string>();
        internal Dictionary<PropertyInfo, Type> UnderlyingPropType { get; set; } = new Dictionary<PropertyInfo, Type>();

        internal DBTypeInfo(Type typ) : base(typ)
        {
            GatherTypeInfo();
        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            PropertyNames.Add(propInfo.Name);
            AddColumnMappingAttribute(propInfo);
            AddUnderlyingType(propInfo);

        }

        private void AddColumnMappingAttribute(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
            if (attr != null)
                ColumnMap2Property.Add(attr.ColumnName, propInfo.Name);
        }

        private void AddUnderlyingType(PropertyInfo propInfo)
        {
            Type t = TypeInfo.TryGetUnderlyingType(propInfo);
            UnderlyingPropType.Add(propInfo, t);
        }

        internal bool HasPropertyOrColumnMapping(string name)
        {
            if (ColumnMap2Property.ContainsKey(name))
                return true;
            else
                return PropertyNames.Any(propName => propName == name);
        }
        internal PropertyInfo GetInfoByPropertyNameOrColumnMapping(string propNameOrColMapName)
        {
            PropertyInfo result = null;
            if (ColumnMap2Property.ContainsKey(propNameOrColMapName))
                result = Properties[PropertyIndex[ColumnMap2Property[propNameOrColMapName]]];
            else
                result = Properties[PropertyIndex[propNameOrColMapName]];
            return result;
        }

        internal int GetIndexByPropertyNameOrColumnMapping(string propNameOrColMapName)
        {
            if (ColumnMap2Property.ContainsKey(propNameOrColMapName))
                return PropertyIndex[ColumnMap2Property[propNameOrColMapName]];
            else
                return PropertyIndex[propNameOrColMapName];
        }
    }
}

