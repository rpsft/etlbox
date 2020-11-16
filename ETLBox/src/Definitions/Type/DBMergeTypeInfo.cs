using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal class DBMergeTypeInfo : TypeInfo
    {
        internal List<string> IdColumnNames { get; set; } = new List<string>();
        internal List<string> UpdateColumnNames { get; set; } = new List<string>();
        internal List<PropertyInfo> IdAttributeProps { get; } = new List<PropertyInfo>();
        internal List<PropertyInfo> UpdateAttributeProps { get; } = new List<PropertyInfo>();
        internal List<Tuple<PropertyInfo, object>> DeleteAttributeProps { get; } = new List<Tuple<PropertyInfo, object>>();
        internal PropertyInfo ChangeDateProperty { get; set; }
        internal PropertyInfo ChangeActionProperty { get; set; }
        internal MergeProperties MergeProps { get; set; }

        internal DBMergeTypeInfo(Type typ, MergeProperties mergeProps) : base(typ)
        {
            MergeProps = mergeProps;
            GatherTypeInfo();
        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddMergeIdColumnNameAttribute(propInfo);
            AddMergeUpdateColumnNameAttribute(propInfo);
            AddIdAddAttributeProps(propInfo);
            AddUpdateAttributeProps(propInfo);
            AddDeleteAttributeProps(propInfo);
            AddChangeActionProp(propInfo);
            AddChangeDateProp(propInfo);
        }

        private void AddMergeIdColumnNameAttribute(PropertyInfo propInfo)
        {
            if (MergeProps.IdPropertyNames.Any(idcol => idcol.IdPropertyName == propInfo.Name))
                IdColumnNames.Add(propInfo.Name);
            else
            {
                var attr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
                if (attr != null)
                {
                    var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                    if (cmattr != null)
                        IdColumnNames.Add(cmattr.NewName);
                    else
                        IdColumnNames.Add(propInfo.Name);
                }
            }
        }

        private void AddMergeUpdateColumnNameAttribute(PropertyInfo propInfo)
        {
            if (MergeProps.UpdatePropertyNames.Any(compcol => compcol.UpdatePropertyName == propInfo.Name))
                UpdateColumnNames.Add(propInfo.Name);
            else
            {
                var attr = propInfo.GetCustomAttribute(typeof(UpdateColumn)) as UpdateColumn;
                if (attr != null)
                {
                    var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                    if (cmattr != null)
                        UpdateColumnNames.Add(cmattr.NewName);
                    else
                        UpdateColumnNames.Add(propInfo.Name);
                }
            }
        }

        private void AddIdAddAttributeProps(PropertyInfo propInfo)
        {
            if (MergeProps.IdPropertyNames.Any(idcol => idcol.IdPropertyName == propInfo.Name))
                IdAttributeProps.Add(propInfo);
            else
            {
                var idAttr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
                if (idAttr != null)
                    IdAttributeProps.Add(propInfo);
            }
        }

        private void AddUpdateAttributeProps(PropertyInfo propInfo)
        {
            if (MergeProps.UpdatePropertyNames.Any(compcol => compcol.UpdatePropertyName == propInfo.Name))
                UpdateAttributeProps.Add(propInfo);
            else
            {
                var updateAttr = propInfo.GetCustomAttribute(typeof(UpdateColumn)) as UpdateColumn;
                if (updateAttr != null)
                    UpdateAttributeProps.Add(propInfo);
            }
        }

        private void AddDeleteAttributeProps(PropertyInfo propInfo)
        {
            if (MergeProps.DeletionProperties.Any(delcol => delcol.DeletePropertyName == propInfo.Name))
                DeleteAttributeProps.Add(Tuple.Create(propInfo, 
                    MergeProps.DeletionProperties.Where( delcol => delcol.DeletePropertyName == propInfo.Name).First().DeleteOnMatchValue));
            else
            {
                var deleteAttr = propInfo.GetCustomAttribute(typeof(DeleteColumn)) as DeleteColumn;
                if (deleteAttr != null)
                    DeleteAttributeProps.Add(Tuple.Create(propInfo, deleteAttr.DeleteOnMatchValue));
            }
        }
        private void AddChangeActionProp(PropertyInfo propInfo)
        {
            if (propInfo.Name.ToLower() == MergeProps.ChangeActionPropertyName.ToLower())
                ChangeActionProperty = propInfo;
        }

        private void AddChangeDateProp(PropertyInfo propInfo)
        {
            if (propInfo.Name.ToLower() == MergeProps.ChangeDatePropertyName.ToLower())
                ChangeDateProperty = propInfo;
        }

    }
}

