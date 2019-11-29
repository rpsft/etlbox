using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    public class MergeableRow : IMergeableRow
    {
        private static ConcurrentDictionary<Type, List<PropertyInfo>> IdAttributeProps { get; }
            = new ConcurrentDictionary<Type, List<PropertyInfo>>();

        public MergeableRow()
        {
            Type curType = this.GetType();
            List<PropertyInfo> curIdAttributeProps;

            if (!IdAttributeProps.TryGetValue(curType, out curIdAttributeProps))
            {
                lock (this)
                {
                    curIdAttributeProps = new List<PropertyInfo>();
                    foreach (PropertyInfo propInfo in curType.GetProperties())
                    {
                        var attr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
                        if (attr != null)
                            curIdAttributeProps.Add(propInfo);
                    }
                    IdAttributeProps.TryAdd(curType, curIdAttributeProps);
                }
            }
        }

        public DateTime ChangeDate { get; set; }
        public string ChangeAction { get; set; }
        public string UniqueId
        {
            get
            {
                List<PropertyInfo> idAttributes = IdAttributeProps[this.GetType()];
                string result = "";
                foreach (var propInfo in idAttributes)
                    result += propInfo?.GetValue(this);
                return result;
            }
        }
    }
}
