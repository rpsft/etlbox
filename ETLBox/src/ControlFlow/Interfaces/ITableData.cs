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
    }
}
