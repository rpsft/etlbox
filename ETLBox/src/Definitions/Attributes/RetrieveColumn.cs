using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines that this property is used to store the lookup value of the property from the object
    /// used in the Source for a Lookup identified by the given lookupSourcePropertyName.
    /// </summary>
    /// <example>
    /// <code>
    /// public class MyLookupData
    /// {
    ///     [MatchColumn("Id")]
    ///     public string LookupId { get; set; }
    ///     [RetrieveColumn("Value")]
    ///     public string LookupValue { get; set; }
    /// }
    ///
    /// public class MyDataRow
    /// {
    ///     public string Id { get; set; }    
    ///     public string Value { get; set; }
    /// }
    /// </code>
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class RetrieveColumn : Attribute
    {
        /// <summary>
        /// Name of the property in the input data
        /// </summary>
                
        public string InputPropertyName { get; set; }

        /// <summary>
        /// Name of the corresponding column in the lookup object.
        /// </summary>
        public string LookupSourcePropertyName { get; set; }

        /// <summary>
        /// This property is used to store the retrieved value from the lookup data.
        /// </summary>
        /// <param name="inputPropertyName">Name of the property in the lookup object</param>
        public RetrieveColumn(string inputPropertyName)
        {
            InputPropertyName = inputPropertyName;
        }

        public RetrieveColumn()
        {

        }
    }
}
