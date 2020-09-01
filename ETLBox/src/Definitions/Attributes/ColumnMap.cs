using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines a column name to which the value of the property is mapped when writing or reading
    /// from a database.
    /// By default, when reading from a database source the mapping which property stores data of which column
    /// is resolved by the property names. Using this attribute, you can specify the column name that maps to a propety.
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
        /// Name of the column in the database
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Create a mapping between the current property and a database column
        /// </summary>
        /// <param name="columnName">Name of the column in the database</param>
        public ColumnMap(string columnName)
        {
            ColumnName = columnName;
        }
    }
}
