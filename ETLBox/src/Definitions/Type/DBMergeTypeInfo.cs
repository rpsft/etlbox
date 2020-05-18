using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class DBMergeTypeInfo : TypeInfo
    {
        internal List<string> IdColumnNames { get; set; } = new List<string>();
        internal List<PropertyInfo> IdAttributeProps { get; } = new List<PropertyInfo>();
        internal List<PropertyInfo> CompareAttributeProps { get; } = new List<PropertyInfo>();
        internal List<Tuple<PropertyInfo, object>> DeleteAttributeProps { get; } = new List<Tuple<PropertyInfo, object>>();


        internal DBMergeTypeInfo(Type typ) : base(typ)
        {

        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddMergeIdColumnNameAttribute(propInfo);
            AddIdAddAttributeProps(propInfo);
            AddCompareAttributeProps(propInfo);
            AddDeleteAttributeProps(propInfo);
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

        private void AddIdAddAttributeProps(PropertyInfo propInfo)
        {
            var idAttr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
            if (idAttr != null)
                IdAttributeProps.Add(propInfo);
        }

        private void AddCompareAttributeProps(PropertyInfo propInfo)
        {
            var compAttr = propInfo.GetCustomAttribute(typeof(CompareColumn)) as CompareColumn;
            if (compAttr != null)
                CompareAttributeProps.Add(propInfo);
        }

        private void AddDeleteAttributeProps(PropertyInfo propInfo)
        {
            var deleteAttr = propInfo.GetCustomAttribute(typeof(DeleteColumn)) as DeleteColumn;
            if (deleteAttr != null)
                DeleteAttributeProps.Add(Tuple.Create(propInfo, deleteAttr.DeleteOnMatchValue));
        }
    }
}

