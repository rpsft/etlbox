using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal class DBMergeTypeInfo : TypeInfo
    {
        internal List<string> IdColumnNames { get; set; } = new List<string>();
        internal List<string> CompareColumnNames { get; set; } = new List<string>();
        internal List<string> UpdateColumnNames { get; set; } = new List<string>();
        internal List<string> NonIdColumnNames { get; set; } = new List<string>();
        internal List<PropertyInfo> IdAttributeProps { get; } = new List<PropertyInfo>();
        internal List<PropertyInfo> CompareAttributeProps { get; } = new List<PropertyInfo>();
        internal List<PropertyInfo> UpdateAttributeProps { get; } = new List<PropertyInfo>();
        internal List<PropertyInfo> NonIdAttributeProps { get; } = new List<PropertyInfo>();
        internal List<Tuple<PropertyInfo, object>> DeleteAttributeProps { get; } = new List<Tuple<PropertyInfo, object>>();
        internal PropertyInfo ChangeDateProperty { get; set; }
        internal PropertyInfo ChangeActionProperty { get; set; }
        internal MergeProperties MergeProps { get; set; }

        internal TableDefinition DestinationTableDefintion { get; set; }

        internal DBMergeTypeInfo(Type typ, MergeProperties mergeProps, TableDefinition destTableDef) : base(typ)
        {
            MergeProps = mergeProps;
            DestinationTableDefintion = destTableDef;
            GatherTypeInfo();
        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddMergeIdColumnNameAttribute(propInfo);
            AddMergeCompareColumnNameAttribute(propInfo);
            AddMergeUpdateColumnNameAttribute(propInfo);
            AddMergeNonIdColumnNameAttribute(propInfo);
            //AddIdAttributeProps(propInfo);
            //AddCompareAttributeProps(propInfo);
            //AddUpdateAttributeProps(propInfo);
            //AddNonIdAttributeProps(propInfo);
            AddDeleteAttributeProps(propInfo);
            AddChangeActionProp(propInfo);
            AddChangeDateProp(propInfo);
        }

        private void AddMergeIdColumnNameAttribute(PropertyInfo propInfo)
        {
            if (MergeProps.IdPropertyNames.Any(idcol => idcol.IdPropertyName == propInfo.Name))
            {
                var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                if (cmattr != null)
                    IdColumnNames.Add(cmattr.NewName);
                else
                    IdColumnNames.Add(propInfo.Name);
                IdAttributeProps.Add(propInfo);
            }
            else
            {
                var attr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
                if (attr != null)
                {
                    IdAttributeProps.Add(propInfo);
                    var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                    if (cmattr != null)
                        IdColumnNames.Add(cmattr.NewName);
                    else
                        IdColumnNames.Add(propInfo.Name);
                }
            }
        }

        private void AddMergeCompareColumnNameAttribute(PropertyInfo propInfo)
        {
            if (MergeProps.ComparePropertyNames.Any(compcol => compcol.ComparePropertyName == propInfo.Name))
            {
                var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                if (cmattr != null)
                    CompareColumnNames.Add(cmattr.NewName);
                else
                    CompareColumnNames.Add(propInfo.Name);
                //CompareColumnNames.Add(propInfo.Name);
                CompareAttributeProps.Add(propInfo);
            }
            else
            {
                var attr = propInfo.GetCustomAttribute(typeof(CompareColumn)) as CompareColumn;
                if (attr != null)
                {
                    CompareAttributeProps.Add(propInfo);
                    var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                    if (cmattr != null)
                        CompareColumnNames.Add(cmattr.NewName);
                    else
                        CompareColumnNames.Add(propInfo.Name);
                }
            }
        }

        private void AddMergeUpdateColumnNameAttribute(PropertyInfo propInfo)
        {
            if (MergeProps.UpdatePropertyNames.Any(compcol => compcol.UpdatePropertyName == propInfo.Name))
            {
                var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                if (cmattr != null)
                    UpdateColumnNames.Add(cmattr.NewName);
                else
                    UpdateColumnNames.Add(propInfo.Name);
                //UpdateColumnNames.Add(propInfo.Name);
                UpdateAttributeProps.Add(propInfo);
            }
            else
            {
                var attr = propInfo.GetCustomAttribute(typeof(UpdateColumn)) as UpdateColumn;
                if (attr != null)
                {
                    UpdateAttributeProps.Add(propInfo);
                    var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                    if (cmattr != null)
                        UpdateColumnNames.Add(cmattr.NewName);
                    else
                        UpdateColumnNames.Add(propInfo.Name);
                }
            }
        }

        private void AddMergeNonIdColumnNameAttribute(PropertyInfo propInfo)
        {
            if (MergeProps.IdPropertyNames.Any(compcol => compcol.IdPropertyName == propInfo.Name))
                return;
            else
            {
                var attr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
                if (attr == null)
                {                    
                    var cmattr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
                    if (cmattr != null)
                    {
                        if (DestinationTableDefintion.Columns.Any(col => col.Name == cmattr.NewName))
                        {
                            NonIdColumnNames.Add(cmattr.NewName);
                            NonIdAttributeProps.Add(propInfo);
                        }
                    }
                    else
                    {
                        if (DestinationTableDefintion.Columns.Any(col => col.Name == propInfo.Name))
                        {
                            NonIdColumnNames.Add(propInfo.Name);
                            NonIdAttributeProps.Add(propInfo);
                        }
                    }
                }
            }
        }

        //private void AddIdAttributeProps(PropertyInfo propInfo)
        //{
        //    if (MergeProps.IdPropertyNames.Any(idcol => idcol.IdPropertyName == propInfo.Name))
        //        IdAttributeProps.Add(propInfo);
        //    else
        //    {
        //        var idAttr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
        //        if (idAttr != null)
        //            IdAttributeProps.Add(propInfo);
        //    }
        //}

        //private void AddCompareAttributeProps(PropertyInfo propInfo)
        //{
        //    if (MergeProps.ComparePropertyNames.Any(compcol => compcol.ComparePropertyName == propInfo.Name))
        //        CompareAttributeProps.Add(propInfo);
        //    else
        //    {
        //        var compAttr = propInfo.GetCustomAttribute(typeof(CompareColumn)) as CompareColumn;
        //        if (compAttr != null)
        //            CompareAttributeProps.Add(propInfo);
        //    }
        //}

        //private void AddUpdateAttributeProps(PropertyInfo propInfo)
        //{
        //    if (MergeProps.UpdatePropertyNames.Any(compcol => compcol.UpdatePropertyName == propInfo.Name))
        //        UpdateAttributeProps.Add(propInfo);
        //    else
        //    {
        //        var updateAttr = propInfo.GetCustomAttribute(typeof(UpdateColumn)) as UpdateColumn;
        //        if (updateAttr != null)
        //            UpdateAttributeProps.Add(propInfo);
        //    }
        //}


        private void AddDeleteAttributeProps(PropertyInfo propInfo)
        {
            if (MergeProps.DeletionProperties.Any(delcol => delcol.DeletePropertyName == propInfo.Name))
                DeleteAttributeProps.Add(Tuple.Create(propInfo,
                    MergeProps.DeletionProperties.Where(delcol => delcol.DeletePropertyName == propInfo.Name).First().DeleteOnMatchValue));
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

