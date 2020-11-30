using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute is used to identify distinct properties in an object.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class DistinctColumn : Attribute
    {
        /// <summary>
        /// Property name used in the object to identify distinct values
        /// </summary>
        public string DistinctPropertyName { get; set; }

        public DistinctColumn()
        {

        }
    }
}
