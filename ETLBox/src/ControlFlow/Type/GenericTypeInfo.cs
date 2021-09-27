using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace ETLBox.DataFlow
{
    [Flags]
    public enum AttributeType
    {
        None = 0,
        ColumnMap = 1,
        IdColumn = 2,
        CompareColumn = 4,
        UpdateColumn = 8,
        DeleteColumn = 16,
        AggregateColumn = 32,
        GroupColumn = 64,
        DistinctColumn = 128,
        MatchColumn = 256,
        RetrieveColumn = 512,
        RenameColumn = 1024,
        KeyColumn = 2048
    }

    public class GenericTypeInfo
    {
        /// <summary>
        /// Indicates if the type is an strong type object (Plain old CLR object)
        /// </summary>
        public bool IsPoco => !IsArray && !IsDynamic;

        /// <summary>
        /// Indicates if the type is an array (e.g. string[])
        /// </summary>
        public bool IsArray { get; private set; }

        /// <summary>
        /// Indicates if the type is an ExpandoObject
        /// </summary>
        public bool IsDynamic { get; private set; }

        /// <summary>
        /// Property info of the type
        /// </summary>
        public List<PropertyInfo> Properties { get; private set; }

        /// <summary>
        /// Property names of the type
        /// </summary>
        public List<string> PropertyNames { get; private set; } = new List<string>();

        /// <summary>
        /// Property info of the type by property name
        /// </summary>
        public Dictionary<string, PropertyInfo> PropertiesByName { get; private set; } = new Dictionary<string, PropertyInfo>();
        public Dictionary<PropertyInfo, Type> UnderlyingPropType { get; private set; } = new Dictionary<PropertyInfo, Type>();

        public List<ColumnMap> ColumnMapAttributes { get; set; } = new List<ColumnMap>();
 

        public GenericTypeInfo(Type type) {
            GenericType = type;
        }

        private Type GenericType { get; set; }

        public GenericTypeInfo GatherTypeInfo(AttributeType propertyAttributes = AttributeType.None) {
            DetermineObjectType();
            if (IsPoco) {
                Properties = GenericType.GetProperties().ToList();
                for (int i = 0; i < Properties.Count; i++) {
                    var propInfo = Properties[i];
                    ThrowIfPropIsIndexer(propInfo);
                    PropertyNames.Add(propInfo.Name);
                    PropertiesByName.Add(propInfo.Name, propInfo);
                    UnderlyingPropType.Add(propInfo, TryGetUnderlyingType(propInfo));
                    TryExtractAttribute(propertyAttributes, propInfo);
                    RetrieveAdditionalTypeInfo(propInfo, i);
                }
            }
            return this;
        }

        private void DetermineObjectType() {
            IsArray = GenericType.IsArray;
            if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(GenericType))
                IsDynamic = true;
        }


        private void ThrowIfPropIsIndexer(PropertyInfo propInfo) {
            if (propInfo.GetIndexParameters().Length > 0)
                throw new NotSupportedException("ETLBox: Index properties (like string this[int i]) are not allow on objects" +
                    " which are used as types for DataFlow components!");
        }

        private void TryExtractAttribute(AttributeType propertyAttributes, PropertyInfo propInfo) {
            if (propertyAttributes == AttributeType.None) return;
            if (propertyAttributes.HasFlag(AttributeType.ColumnMap))
                TryAddAttribute(propInfo, ColumnMapAttributes, (cm, prop) => cm.PropertyName = prop.Name);
        }

        private void TryAddAttribute<T>(PropertyInfo propInfo, IList<T> list, Action<T, PropertyInfo> propAssignment) where T : Attribute {
            T attr = propInfo.GetCustomAttribute(typeof(T)) as T;
            if (attr != null) {
                propAssignment(attr, propInfo);
                list.Add(attr);
            }
        }

        protected virtual void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex) {
            ;
        }

        public static Type TryGetUnderlyingType(PropertyInfo propInfo)
            => TryGetUnderlyingType(propInfo.PropertyType);

        public static Type TryGetUnderlyingType(Type type)
            => Nullable.GetUnderlyingType(type) ?? type;

        /// <summary>
        /// Determines if a type is numeric.  Nullable numeric types are considered numeric.        
        /// </summary>
        /// <remarks>
        /// Boolean is not considered numeric.
        /// http://stackoverflow.com/a/5182747/172132
        /// </remarks>        
        public static bool IsNumericType(Type type) {
            if (type == null) {
                return false;
            }

            switch (Type.GetTypeCode(type)) {
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
                    if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>)) {
                        return IsNumericType(Nullable.GetUnderlyingType(type));
                    }
                    return false;
            }
            return false;
        }
    }
}

