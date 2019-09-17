using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace ALE.ETLBox {

    public interface ITableData : IDisposable, IDataReader {
        IColumnMappingCollection ColumnMapping { get; }
        List<object[]> Rows { get; }
    }
}
