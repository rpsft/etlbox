using ETLBox.Connection;
using System;
using System.ComponentModel;
using System.Data;
using System.Text.RegularExpressions;

namespace ETLBox.Helper
{
    /// <summary>
    /// Describe methods that allows to intercept the CREATE TABLE sql generation in a CreateTableTask.
    /// It will convert the data type that is defined in a TableColumn into a custom database specific type.
    /// </summary>
    public interface IDataTypeConverter
    {
        /// <summary>
        /// Tries to convert the data type from the TableColumn into a database specific type.
        /// </summary>
        /// <param name="dataTypeName">The specific type name from a table column</param>
        /// <param name="connectionType">The database connection type</param>
        /// <returns>The type used in the CREATE TABLE statement</returns>
        string TryConvertDbDataType(string dataTypeName, ConnectionManagerType connectionType);
    }
}
