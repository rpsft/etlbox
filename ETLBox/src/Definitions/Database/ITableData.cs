using System.Data;

namespace ALE.ETLBox
{
    public interface ITableData : IDisposable, IDataReader
    {
        IColumnMappingCollection ColumnMapping { get; }
        List<object[]> Rows { get; }
    }
}
