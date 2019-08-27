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
        internal Dictionary<string, string> ColumnMap2Property { get; set; }
        internal Dictionary<int, int> ExcelIndex2PropertyIndex { get; set; }
        internal string MergeIdColumnName { get; set; }
        internal int PropertyLength { get; set; }
        internal bool IsArray { get; set; } = true;

        internal TypeInfo(Type typ)
        {
            PropertyNames = new List<string>();
            PropertyIndex = new Dictionary<string, int>();
            ColumnMap2Property = new Dictionary<string, string>();
            ExcelIndex2PropertyIndex = new Dictionary<int, int>();
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
                    PropertyIndex.Add(propInfo.Name, index);
                    AddColumnMappingAttribute(propInfo);
                    AddExcelColumnAttribute(propInfo, index);
                    AddMergeIdColumnNameAttribute(propInfo);
                    index++;
                }
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
            var attr = propInfo.GetCustomAttribute(typeof(MergeIdColumnName)) as MergeIdColumnName;
            if (attr != null)
                MergeIdColumnName = attr.IdColumnName;
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
        internal PropertyInfo GetInfoByPropertyNameOrColumnMapping(string propNameOrColMapName) {
            if (ColumnMap2Property.ContainsKey(propNameOrColMapName))
                return Properties[PropertyIndex[ColumnMap2Property[propNameOrColMapName]]];
            else
                return Properties[PropertyIndex[propNameOrColMapName]];
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

