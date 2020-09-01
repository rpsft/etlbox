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
        /// Property name used for groupin in the aggregation output object.
        /// </summary>
        public string AggregationGroupingProperty { get; set; }

        /// <summary>
        /// Sets the property name used for grouping in the aggregation output object
        /// </summary>
        /// <param name="aggregationGroupingProperty">Property name used for grouping in the output object</param>
        public GroupColumn(string aggregationGroupingProperty)
        {
            AggregationGroupingProperty = aggregationGroupingProperty;
        }
    }
}
