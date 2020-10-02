using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute is used to identify the grouping property for aggregations. The passed column name
    /// identifies the property in the aggregation output object.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class GroupColumn : Attribute
    {
        /// <summary>
        /// Property name used in the input object for grouping the aggregation.
        /// </summary>
        public string InputGroupingProperty { get; set; }

        /// <summary>
        /// Property name that holds the grouping value in the output object
        /// </summary>
        public string OutputGroupingProperty { get; set; }

        /// <summary>
        /// Sets the property name used for grouping in the input object
        /// </summary>
        /// <param name="inputGroupingProperty">Property name in the input object used for grouping the aggregation data</param>
        public GroupColumn(string inputGroupingProperty)
        {
            InputGroupingProperty = inputGroupingProperty;
        }

        public GroupColumn()
        {

        }
    }
}
