using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines a column name to which the value of the property is mapped when writing or reading
    /// from a database or using the ColumnRename transformation.
    /// By default, when reading from a database source the mapping which property stores data of which column
    /// is resolved by the property names. Using this attribute, you can specify the column name that maps to a property.
    /// In the ColumnRename transformation, this property is used to rename a column
    /// </summary>
    /// <example>
    ///  public class MyPoco
    /// {
    ///     [ColumnMap("Column1")]
    ///     public string MyProperty { get; set; }
    /// }
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnMap : Attribute
    {
        /// <summary>
        /// Name of the column in the database or the new name when renaming
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Create a mapping between the current property and a column        
        /// </summary>
        /// <param name="columnName">Name of the column in the database or the new name when renaming</param>
        public ColumnMap(string columnName)
        {
            ColumnName = columnName;
        }
    }
}
