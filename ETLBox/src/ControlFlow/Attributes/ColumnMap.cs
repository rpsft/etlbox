using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines the mapping between property names in the objects and column names
    /// in the database. 
    /// By default, when reading or writing data from/into a database, properties in your objects
    /// are mapped to database columns with the same name (case-sensitive). Using this attribute, you can 
    /// add your own mapping. (E.g. a property "Prop1" is mapped by default to the database column "Prop1".
    /// Create a column mapping to change the mapping to "Column1")
    /// </summary>
    /// <example>
    /// <code>
    ///  public class MyPoco
    /// {
    ///     [ColumnMap("Column1")]
    ///     public string Prop1 { get; set; }
    /// }
    /// </code>
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ColumnMap : Attribute
    {
        /// <summary>
        /// Name of the database column
        /// </summary>
        public string DbColumnName { get; set; }

        public ColumnMap() {

        }

        /// <summary>
        /// Creates a mapping between the a property and a database column        
        /// </summary>
        /// <param name="dbColumnName">The name of the column in the database</param>
        public ColumnMap(string dbColumnName) {
            DbColumnName = dbColumnName;
        }

        /// <summary>
        /// Index of the element in the array, only necessary if you use arrays 
        /// as data type
        /// </summary>
        // int? is only ok because this property can't be used as an attribute decorator - arrays info can only
        // be set manually. Otherwise a nullable would be an issue: https://etlbox.atlassian.net/browse/EB-128
        public int? ArrayIndex { get; set; }

        /// <summary>
        /// Name of the property that should be mapped to a database column
        /// </summary>
        public string PropertyName { get; set; }

        /// <summary>
        /// If set to true, this property will be ignored and not mapped to any column. 
        /// </summary>
        public bool IgnoreColumn { get; set; }
    }
}
