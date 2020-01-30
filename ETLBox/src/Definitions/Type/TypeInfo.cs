using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class TypeInfo
    {
        internal PropertyInfo[] Properties { get; set; }
        internal List<string> PropertyNames { get; set; } = new List<string>();
        internal Dictionary<string, int> PropertyIndex { get; set; } = new Dictionary<string, int>();
        internal Dictionary<string, string> ColumnMap2Property { get; set; } = new Dictionary<string, string>();
        internal Dictionary<int, int> ExcelIndex2PropertyIndex { get; set; } = new Dictionary<int, int>();
        internal Dictionary<PropertyInfo, Type> UnderlyingPropType { get; set; } = new Dictionary<PropertyInfo, Type>();
        internal Dictionary<string, PropertyInfo> PropertiesByName { get; set; } = new Dictionary<string, PropertyInfo>();
        internal List<Tuple<PropertyInfo, string>> MatchColumns { get; set; } = new List<Tuple<PropertyInfo, string>>();
        internal List<Tuple<PropertyInfo, string>> RetrieveColumns { get; set; } = new List<Tuple<PropertyInfo, string>>();
        internal List<string> IdColumnNames { get; set; } = new List<string>();
        internal int PropertyLength { get; set; }
        internal bool IsArray { get; set; } = true;
        internal bool IsDynamic { get; set; }
        internal int ArrayLength { get; set; }

        internal TypeInfo(Type typ)
        {
            IsArray = typ.IsArray;
            if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(typ))
                IsDynamic = true;
            if (!IsArray && !IsDynamic)
            {
                Properties = typ.GetProperties();
                PropertyLength = Properties.Length;
                int index = 0;
                foreach (var propInfo in Properties)
                {
                    PropertyNames.Add(propInfo.Name);
                    PropertiesByName.Add(propInfo.Name, propInfo);
                    PropertyIndex.Add(propInfo.Name, index);
                    AddColumnMappingAttribute(propInfo);
                    AddExcelColumnAttribute(propInfo, index);
                    AddMergeIdColumnNameAttribute(propInfo);
                    AddUnderlyingType(propInfo);
                    AddRetrieveColumn(propInfo);
                    AddMatchColumn(propInfo);
                    index++;
                }
            }
            else if (IsArray)
            {
                ArrayLength = typ.GetArrayRank();
            }
        }

        private void AddColumnMappingAttribute(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
            if (attr != null)
                ColumnMap2Property.Add(attr.ColumnName, propInfo.Name);
        }

        private void AddExcelColumnAttribute(PropertyInfo propInfo, int curIndex)
        {
            var attr = propInfo.GetCustomAttribute(typeof(ExcelColumn)) as ExcelColumn;
            if (attr != null)
                ExcelIndex2PropertyIndex.Add(attr.Index, curIndex);
        }

        private void AddMergeIdColumnNameAttribute(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
            if (attr != null)
            {
                var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                if (cmattr != null)
                    IdColumnNames.Add(cmattr.ColumnName);
                else
                    IdColumnNames.Add(propInfo.Name);
            }
        }

        private void AddUnderlyingType(PropertyInfo propInfo)
        {
            Type t = Nullable.GetUnderlyingType(propInfo.PropertyType) ?? propInfo.PropertyType;
            UnderlyingPropType.Add(propInfo, t);
        }

        private void AddMatchColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(MatchColumn)) as MatchColumn;
            if (attr != null)
                MatchColumns.Add(Tuple.Create(propInfo, attr.LookupSourcePropertyName));
        }

        private void AddRetrieveColumn(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(RetrieveColumn)) as RetrieveColumn;
            if (attr != null)
                RetrieveColumns.Add(Tuple.Create(propInfo, attr.LookupSourcePropertyName));
        }

        internal static object CastPropertyValue(PropertyInfo property, string value)
        {
            if (property == null || String.IsNullOrEmpty(value))
                return null;
            if (property.PropertyType == typeof(bool))
                return value == "1" || value == "true" || value == "on" || value == "checked";
            else
            {
                Type t = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
                return Convert.ChangeType(value, t);
            }
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

