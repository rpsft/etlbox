namespace ALE.ETLBox.src.Definitions.Database
{
    public interface ITableData : IDataReader
    {
        IColumnMappingCollection GetColumnMapping();
        List<object[]> Rows { get; }
    }
}
