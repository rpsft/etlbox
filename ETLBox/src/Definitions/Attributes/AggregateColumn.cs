using ETLBox.DataFlow.Transformations;
using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute is used to identify the aggregation property for aggregations.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class AggregateColumn : Attribute
    {
        /// <summary>
        /// Property name in the input object that contains the detailed values.
        /// </summary>
        public string InputProperty { get; set; }

        /// <summary>
        /// Method for the aggregation (e.g. Sum, Min, Max, etc.)
        /// </summary>
        public AggregationMethod AggregationMethod { get; set; }
        
        /// <summary>
        /// Property name that holds the aggregated value in the output object
        /// </summary>
        public string AggregationProperty { get; set; }

        /// <summary>
        /// Sets the property name in the aggregation output object and the aggregation method
        /// </summary>
        /// <param name="inputProperty">Property name in the input object that contains the detailed values.</param>
        /// <param name="aggregationMethod">Method for the aggregation (e.g. Sum, Min, Max, etc.)</param>
        public AggregateColumn(string inputProperty, AggregationMethod aggregationMethod)
        {
            InputProperty = inputProperty;
            AggregationMethod = aggregationMethod;
        }

        public AggregateColumn()
        {

        }
    }
}
