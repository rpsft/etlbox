namespace ALE.ETLBox.src.Definitions.DataFlow.Type
{
    internal sealed class DBTypeInfo : TypeInfo
    {
        internal List<string> PropertyNames { get; } = new();
        internal Dictionary<PropertyInfo, System.Type> UnderlyingPropType { get; } = new();
        private Dictionary<string, string> ColumnMap2Property { get; } = new();

        internal DBTypeInfo(System.Type type)
            : base(type)
        {
            GatherTypeInfo();
        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            PropertyNames.Add(propInfo.Name);
            AddColumnMappingAttribute(propInfo);
            AddUnderlyingType(propInfo);
        }

        private void AddColumnMappingAttribute(MemberInfo propInfo)
        {
            if (propInfo.GetCustomAttribute(typeof(ColumnMapAttribute)) is ColumnMapAttribute attr)
                ColumnMap2Property.Add(attr.ColumnName, propInfo.Name);
        }

        private void AddUnderlyingType(PropertyInfo propInfo)
        {
            System.Type t = TryGetUnderlyingType(propInfo);
            UnderlyingPropType.Add(propInfo, t);
        }

        internal bool HasPropertyOrColumnMapping(string name)
        {
            return ColumnMap2Property.ContainsKey(name) || PropertyNames.Contains(name);
        }

        internal PropertyInfo GetInfoByPropertyNameOrColumnMapping(string propNameOrColMapName)
        {
            return ColumnMap2Property.TryGetValue(propNameOrColMapName, out var value)
                ? Properties[PropertyIndex[value]]
                : Properties[PropertyIndex[propNameOrColMapName]];
        }

        internal int GetIndexByPropertyNameOrColumnMapping(string propNameOrColMapName)
        {
            return ColumnMap2Property.TryGetValue(propNameOrColMapName, out var value)
                ? PropertyIndex[value]
                : PropertyIndex[propNameOrColMapName];
        }
    }
}
