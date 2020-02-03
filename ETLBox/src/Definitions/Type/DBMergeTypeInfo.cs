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

        internal DBMergeTypeInfo(Type typ) : base(typ)
        {

        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddMergeIdColumnNameAttribute(propInfo);
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


    }
}

