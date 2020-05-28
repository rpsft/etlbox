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
        public string AggregationGroupingProperty { get; set; }
        public GroupColumn(string aggregationGroupingProperty)
        {
            AggregationGroupingProperty = aggregationGroupingProperty;
        }
    }
}
