namespace ALE.ETLBox
{
    public interface ITableData : IDataReader
    {
        IColumnMappingCollection GetColumnMapping();
        List<object[]> Rows { get; }
    }
}
