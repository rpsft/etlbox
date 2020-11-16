using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines if the column is included in the comparison to identify
    /// object that exists and needs to be updated. It it supposed
    /// to use with an object that either inherits from MergeableRow.
    /// If you implement the IMergeable interface, you need to override the Equals-method instead.
    /// </summary>
    /// <example>
    ///  public class MyPoco : MergeableRow
    /// {
    ///     [IdColumn]
    ///     public int Key { get; set; }
    ///     [UpdateColumn]
    ///     public string Value {get;set; }
    /// }
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public class UpdateColumn : Attribute
    {
        /// <summary>
        /// Name of the property name used for the comparison if two rows match 
        /// </summary>
        public string UpdatePropertyName { get; set; }

        /// <summary>
        /// Marks this property as column used for comparison in a Merge operation
        /// </summary>
        public UpdateColumn()
        {
        }
    }
}
