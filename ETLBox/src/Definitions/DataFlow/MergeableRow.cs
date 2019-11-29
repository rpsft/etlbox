using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    public class MergeableRow : IMergeableRow
    {
        private static ConcurrentDictionary<Type,AttributeProperties> AttributePropDict { get; }
            = new ConcurrentDictionary<Type, AttributeProperties>();

        public MergeableRow()
        {
            Type curType = this.GetType();
            AttributeProperties curAttrProps;
            if (!AttributePropDict.TryGetValue(curType, out curAttrProps))
            {
                lock (this)
                {
                    curAttrProps = new AttributeProperties();
                    foreach (PropertyInfo propInfo in curType.GetProperties())
                    {
                        var idAttr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
                        if (idAttr != null)
                            curAttrProps.IdAttributeProps.Add(propInfo);
                        var compAttr = propInfo.GetCustomAttribute(typeof(CompareColumn)) as CompareColumn;
                        if (compAttr != null)
                            curAttrProps.CompareAttributeProps.Add(propInfo);
                    }
                    AttributePropDict.TryAdd(curType, curAttrProps);
                }
            }
        }

        public DateTime ChangeDate { get; set; }
        public string ChangeAction { get; set; }
        public string UniqueId
        {
            get
            {
                AttributeProperties attrProps = AttributePropDict[this.GetType()];
                string result = "";
                foreach (var propInfo in attrProps.IdAttributeProps)
                    result += propInfo?.GetValue(this);
                return result;
            }
        }

        public override bool Equals(object other)
        {
            if (other == null) return false;
            AttributeProperties attrProps = AttributePropDict[this.GetType()];
            bool result = true;
            foreach (var propInfo in attrProps.CompareAttributeProps)
                result &= (propInfo?.GetValue(this)).Equals(propInfo?.GetValue(other));
            return result;
        }
    }

    public class AttributeProperties
    {
        public List<PropertyInfo> IdAttributeProps { get; } = new List<PropertyInfo>();
        public List<PropertyInfo> CompareAttributeProps { get; } = new List<PropertyInfo>();
    }
}
