using System;
using System.Collections.Generic;
using System.Data;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// A list of rows and a column mapping ready for bulk insert
    /// </summary>
    public interface ITableData : IDisposable, IDataReader
    {
        /// <summary>
        /// The column mapping
        /// </summary>
        IColumnMappingCollection ColumnMapping { get; }

        /// <summary>
        /// Rows/Columns ready for bulk insert
        /// </summary>
        List<object[]> Rows { get; }

        /// <summary>
        /// The row that is currently processed when accessing the data reader
        /// </summary>
        object[] CurrentRow { get; }

        /// <summary>
        /// The row index of the current row
        /// </summary>
        int ReadIndex { get; }

        /// <summary>
        /// The name of the destination table
        /// </summary>
        string DestinationTableName { get; }

        /// <summary>
        /// Resolves the data type name for a column
        /// </summary>
        /// <param name="columnName">Column name</param>
        /// <returns>Data type name</returns>
        string GetDataTypeName(string columnName);

        /// <summary>
        /// Definition of the destination table
        /// </summary>
        TableDefinition Definition { get; }

        /// <summary>
        /// Resolves the index number for a column name
        /// </summary>
        Dictionary<string, int> DataIndexForColumn { get; set; }

        /// <summary>
        /// If set to true, values in an indentity column are overwriten with the provided values
        /// </summary>
        bool AllowIdentityInsert { get; set; }

    }
}
