using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal class ColumnRenameTypeInfo : TypeInfo
    {
        internal Dictionary<string, ColumnMap> ColumnRenamingDict { get; set; } = new Dictionary<string, ColumnMap>();

        internal ColumnRenameTypeInfo(Type typ) : base(typ)
        {
            GatherTypeInfo();
        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddColumnMappingAttribute(propInfo);

        }

        private void AddColumnMappingAttribute(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(ColumnMap)) as ColumnMap;
            if (attr != null)
                ColumnRenamingDict.Add(propInfo.Name, attr);
        }
    }
}

