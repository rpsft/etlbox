namespace ALE.ETLBox.DataFlow
{
    internal class TypeInfo
    {
        internal PropertyInfo[] Properties { get; set; }
        protected Dictionary<string, int> PropertyIndex { get; set; } = new();
        internal int PropertyLength { get; set; }
        internal bool IsArray { get; set; } = true;
        internal bool IsDynamic { get; set; }
        internal int ArrayLength { get; set; }

        private Type Typ { get; set; }

        internal TypeInfo(Type typ)
        {
            Typ = typ;
        }

        internal TypeInfo GatherTypeInfo()
        {
            IsArray = Typ.IsArray;
            if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(Typ))
                IsDynamic = true;
            if (!IsArray && !IsDynamic)
            {
                Properties = Typ.GetProperties();
                PropertyLength = Properties.Length;
                int index = 0;
                foreach (var propInfo in Properties)
                {
                    PropertyIndex.Add(propInfo.Name, index);
                    RetrieveAdditionalTypeInfo(propInfo, index);
                    index++;
                }
            }
            else if (IsArray)
            {
                ArrayLength = Typ.GetArrayRank();
            }
            return this;
        }

        internal static Type TryGetUnderlyingType(PropertyInfo propInfo)
        {
            return Nullable.GetUnderlyingType(propInfo.PropertyType) ?? propInfo.PropertyType;
        }

        protected virtual void RetrieveAdditionalTypeInfo(
            PropertyInfo propInfo,
            int currentIndex
        ) { }
    }
}
