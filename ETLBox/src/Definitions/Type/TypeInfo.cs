using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// Used to gather information about used type in the data flow component.
    /// </summary>
    public class TypeInfo
    {
        /// <summary>
        /// Property names of the type
        /// </summary>
        public PropertyInfo[] Properties { get; set; }
        /// <summary>
        /// Mapping of property name and its index the <see cref="Properties"/> array.
        /// </summary>
        protected Dictionary<string, int> PropertyIndex { get; set; } = new Dictionary<string, int>();
        internal int PropertyLength { get; set; }

        /// <summary>
        /// Indicates if the type is an array (e.g. string[])
        /// </summary>
        public bool IsArray { get; set; } = true;

        /// <summary>
        /// Indicates if the type is an ExpandoObject
        /// </summary>
        public bool IsDynamic { get; set; }
        internal int ArrayLength { get; set; }

        internal Type Typ { get; set; }
        public TypeInfo(Type typ)
        {
            Typ = typ;
        }

        /// <summary>
        /// Reads the type information from <see cref="Typ"/>.
        /// </summary>
        /// <returns>The TypeInfo object containing information about the Typ.</returns>
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

