using System;
using System.Collections.Generic;
using System.Data;

namespace ETLBox.ControlFlow
{

    public interface ITableData : IDisposable, IDataReader
    {
        IColumnMappingCollection ColumnMapping { get; }
        List<object[]> Rows { get; }
    }
}
