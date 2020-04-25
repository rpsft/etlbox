using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Inherit from this class if you want to use your data object with a DBMerge.
    /// This implementation needs that you have flagged the id properties with the IdColumn attribute
    /// and the properties use to identify equal object flagged with the CompareColumn attribute.
    /// </summary>
    /// <see cref="CompareColumn"/>
    /// <see cref="IdColumn"/>
    public abstract class MergeableRow : IMergeableRow
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
                        var deleteAttr = propInfo.GetCustomAttribute(typeof(DeleteColumn)) as DeleteColumn;
                        if (deleteAttr != null)
                            curAttrProps.DeleteAttributeProps.Add(Tuple.Create(propInfo, deleteAttr.DeleteOnMatchValue));
                    }
                    AttributePropDict.TryAdd(curType, curAttrProps);
                }
            }
        }

        /// <summary>
        /// Date and time when the object was considered for merging.
        /// </summary>
        public DateTime ChangeDate { get; set; }

        /// <summary>
        /// The result of a merge operation - this is either 'I' for Insertion,
        /// 'U' for Updates, 'E' for existing records (no change), and 'D' for deleted records.
        /// </summary>
        public ChangeAction? ChangeAction { get; set; }

        /// <summary>
        /// The UniqueId of the object. This is a concatenation evaluated from the properties
        /// which have the IdColumn attribute. if using an object as type, it is converted into a string
        /// using the ToString() method of the object.
        /// </summary>
        /// <see cref="IdColumn"/>
        public string UniqueId
        {
            get
            {
                AttributeProperties attrProps = AttributePropDict[this.GetType()];
                string result = "";
                foreach (var propInfo in attrProps.IdAttributeProps)
                    result += propInfo?.GetValue(this).ToString();
                return result;
            }
        }

        public bool IsDeletion
        {
            get
            {
                AttributeProperties attrProps = AttributePropDict[this.GetType()];
                bool result = true;
                foreach (var tup in attrProps.DeleteAttributeProps)
                    result &= (tup.Item1?.GetValue(this)).Equals(tup.Item2);
                return result;
            }
        }

        /// <summary>
        /// Overriding the Equals implementation. The Equals function is used identify records
        /// that don't need to be updated. Only properties marked with the CompareColumn attribute
        /// are considered for the comparison. If the property is of type object, the Equals() method of the object is used.
        /// </summary>
        /// <param name="other">Object to compare with. Should be of the same type.</param>
        /// <returns>True if all properties marked with CompareColumn attribute are equal.</returns>
        public override bool Equals(object other)
        {
            if (other == null) return false;
            AttributeProperties attrProps = AttributePropDict[this.GetType()];
            bool result = true;
            foreach (var propInfo in attrProps.CompareAttributeProps)
                result &= (propInfo?.GetValue(this))?.Equals(propInfo?.GetValue(other)) ?? false;
            return result;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }

    public class AttributeProperties
    {
        public List<PropertyInfo> IdAttributeProps { get; } = new List<PropertyInfo>();
        public List<PropertyInfo> CompareAttributeProps { get; } = new List<PropertyInfo>();
        public List<Tuple<PropertyInfo, object>> DeleteAttributeProps { get; } = new List<Tuple<PropertyInfo, object>>();
    }
}
