using System.Collections.Generic;
using System.Data;

namespace ETLBox.Primitives
{
    public interface ITableData : IDataReader
    {
        IColumnMappingCollection GetColumnMapping();
        List<object[]> Rows { get; }
    }
}
