using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines that this property is used to match with the property of the object
    /// used in the Source for a Lookup identified by the given lookupSourcePropertyName.
    /// </summary>
    /// <example>
    /// <code>
    /// public class MyLookupData
    /// {
    ///     public string Id { get; set; }
    ///     public string Value { get; set; }
    /// }
    ///
    /// public class MyDataRow
    /// {
    ///     [MatchColumn("Id")]
    ///     public string MyProperty { get; set; }
    ///     [RetrieveColumn("Value")]
    ///     public string MyProperty { get; set; }
    /// }
    /// </code>
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public class MatchColumn : Attribute
    {
        /// <summary>
        /// Name of the corresponding column in the lookup object.
        /// </summary>
        public string LookupSourcePropertyName { get; set; }

        /// <summary>
        /// This property is used to check if is matches with the lookup data .
        /// </summary>
        /// <param name="lookupSourcePropertyName">Name of the property in the lookup object.</param>
        public MatchColumn(string lookupSourcePropertyName)
        {
            LookupSourcePropertyName = lookupSourcePropertyName;
        }
    }
}
