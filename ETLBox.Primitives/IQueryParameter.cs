using System.Data;

namespace ETLBox.Primitives
{
    public interface IQueryParameter
    {
        string Name { get; }
        string Type { get; }
        object Value { get; }
        DbType DBType { get; }
    }
}