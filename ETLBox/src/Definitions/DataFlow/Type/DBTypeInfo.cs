using System.Linq;

namespace ALE.ETLBox.DataFlow
{
    internal class DBTypeInfo : TypeInfo
    {
        internal List<string> PropertyNames { get; set; } = new();
        internal Dictionary<string, string> ColumnMap2Property { get; set; } = new();
        internal Dictionary<PropertyInfo, Type> UnderlyingPropType { get; set; } = new();

        internal DBTypeInfo(Type typ)
            : base(typ)
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
            Type t = TryGetUnderlyingType(propInfo);
            UnderlyingPropType.Add(propInfo, t);
        }

        internal bool HasPropertyOrColumnMapping(string name)
        {
            if (ColumnMap2Property.ContainsKey(name))
                return true;
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
            return PropertyIndex[propNameOrColMapName];
        }
    }
}
