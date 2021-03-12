using ETLBox.Exceptions;
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

        public Dictionary<string, PropertyInfo> PropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();

        internal int PropertyLength { get; set; }

        /// <summary>
        /// Indicates if the type is an array (e.g. string[])
        /// </summary>
        public bool IsArray { get; set; } = true;

        /// <summary>
        /// Indicates if the type is an ExpandoObject
        /// </summary>
        public bool IsDynamic { get; set; }

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
                    ThrowIfPropIsIndexer(propInfo);
                    PropertyIndex.Add(propInfo.Name, index);
                    PropertiesByName.Add(propInfo.Name, propInfo);
                    RetrieveAdditionalTypeInfo(propInfo, index);
                    index++;
                }
            }
            return this;
        }

        private static void ThrowIfPropIsIndexer(PropertyInfo propInfo)
        {
            if (propInfo.GetIndexParameters().Length > 0)
                throw new ETLBoxNotSupportedException("Index properties (like string this[int i]) are not allow on objects" +
                    " which are used as types for DataFlow components!");
        }

        internal static Type TryGetUnderlyingType(PropertyInfo propInfo)
        {
            return Nullable.GetUnderlyingType(propInfo.PropertyType) ?? propInfo.PropertyType;
        }

        protected virtual void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            ;
        }

        /// <summary>
        /// Determines if a type is numeric.  Nullable numeric types are considered numeric.        
        /// </summary>
        /// <remarks>
        /// Boolean is not considered numeric.
        /// http://stackoverflow.com/a/5182747/172132
        /// </remarks>        
        public static bool IsNumericType(Type type)
        {
            if (type == null)
            {
                return false;
            }

            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Byte:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.SByte:
                case TypeCode.Single:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return true;
                case TypeCode.Object:
                    if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        return IsNumericType(Nullable.GetUnderlyingType(type));
                    }
                    return false;
            }
            return false;
        }

    }
}

