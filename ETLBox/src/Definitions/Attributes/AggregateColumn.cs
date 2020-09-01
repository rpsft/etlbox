using ETLBox.DataFlow.Transformations;
using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute is used to identify the aggregation property for aggregations.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class AggregateColumn : Attribute
    {
        /// <summary>
        /// Property name in the aggregation output object.
        /// </summary>
        public string AggregationProperty { get; set; }

        /// <summary>
        /// Method for the aggreation (e.g. Sum, Min, Max, etc.)
        /// </summary>
        public AggregationMethod AggregationMethod { get; set; }


        /// <summary>
        /// Sets the property name in the aggregation output object and the aggregation method
        /// </summary>
        /// <param name="aggregationProperty">Property name in the aggregation output object.</param>
        /// <param name="aggregationMethod">Method for the aggreation (e.g. Sum, Min, Max, etc.)</param>
        public AggregateColumn(string aggregationProperty, AggregationMethod aggregationMethod)
        {
            AggregationProperty = aggregationProperty;
            AggregationMethod = aggregationMethod;
        }
    }
}
