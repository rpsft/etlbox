using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    public class TypeInfo
    {
        public PropertyInfo[] Properties { get; set; }
        protected Dictionary<string, int> PropertyIndex { get; set; } = new Dictionary<string, int>();
        internal int PropertyLength { get; set; }
        public bool IsArray { get; set; } = true;
        public bool IsDynamic { get; set; }
        internal int ArrayLength { get; set; }

        internal Type Typ { get; set; }
        public TypeInfo(Type typ)
        {
            Typ = typ;
        }

        public TypeInfo GatherTypeInfo()
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

        protected virtual void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            ;
        }
    }
}

