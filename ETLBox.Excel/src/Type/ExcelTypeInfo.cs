using System;
using System.Collections.Generic;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal class ExcelTypeInfo : TypeInfo
    {
        internal Dictionary<int, int> ExcelIndex2PropertyIndex { get; set; } = new Dictionary<int, int>();
        internal Dictionary<string, int> ExcelColumnName2PropertyIndex { get; set; } = new Dictionary<string, int>();

        internal ExcelTypeInfo(Type typ) : base(typ)
        {
            GatherTypeInfo();
        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddExcelColumnAttribute(propInfo, currentIndex);
        }

        private void AddExcelColumnAttribute(PropertyInfo propInfo, int curIndex)
        {
            var attr = propInfo.GetCustomAttribute(typeof(ExcelColumn)) as ExcelColumn;
            if (attr != null)
            {
                if (attr.Index != null)
                    ExcelIndex2PropertyIndex.Add(attr.Index ?? 0, curIndex);
                else if (!string.IsNullOrEmpty(attr.ColumnName))
                    ExcelColumnName2PropertyIndex.Add(attr.ColumnName, curIndex);
            }
            else
                ExcelColumnName2PropertyIndex.Add(propInfo.Name, curIndex);
        }

        internal object CastPropertyValue(PropertyInfo property, string value)
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
    }
}

