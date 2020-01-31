using System;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines that this property is used to match with the property of the object
    /// used in the Source for a Lookup identified by the given lookupSourcePropertyName.
    /// </summary>
    /// <example>
    /// public class MyLookupData
    /// {
    ///     public string Id { get; set; }
    /// }
    ///
    /// public class MyDataRow
    /// {
    ///     [MatchColumn("Id")]
    ///     public string MyProperty { get; set; }
    /// }
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public class RetrieveColumn : Attribute
    {
        public string LookupSourcePropertyName { get; set; }
        public RetrieveColumn(string lookupSourcePropertyName)
        {
            LookupSourcePropertyName = lookupSourcePropertyName;
        }
    }
}
