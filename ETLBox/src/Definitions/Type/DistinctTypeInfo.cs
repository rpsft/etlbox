using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ETLBox.DataFlow
{
    internal class DistinctTypeInfo : TypeInfo
    {
        internal List<string> DistinctColumns { get; set; } = new List<string>();

        internal DistinctTypeInfo(Type typ) : base(typ)
        {
            GatherTypeInfo();
        }

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            AddDistinctAttribute(propInfo);

        }

        private void AddDistinctAttribute(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute(typeof(DistinctColumn)) as DistinctColumn;
            if (attr != null)
                DistinctColumns.Add(propInfo.Name);
        }
    }
}

