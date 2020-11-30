using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines if a column is the destination is updated. 
    /// Use the CompareColumn attribute to define if a row in the source and destination
    /// match. If they match, you can use the UpdateColumn to describe which columns
    /// are actually updated. This is optional - if you don't provide any update columns,
    /// all non id columns are updated. 
    /// </summary>
    /// <example>
    ///  public class MyPoco : MergeableRow
    /// {
    ///     [IdColumn]
    ///     public int Key { get; set; }
    ///     [CompareColumn]
    ///     public string HashValue { get;set; }
    ///     [UpdateColumn]
    ///     public string ValueToUpdate { get;set; }
    ///     public string IgnoredValue { get;set; }
    ///     
    /// }
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class UpdateColumn : Attribute
    {
        /// <summary>
        /// Name of the property that is used in the update
        /// </summary>
        public string UpdatePropertyName { get; set; }
                
        public UpdateColumn()
        {
        }
    }
}
