using ALE.ETLBox.src.Definitions.ConnectionManager;

namespace ALE.ETLBox.src.Definitions.Database
{
    [PublicAPI]
    public class QueryParameter
    {
        public string Name { get; }
        public string Type { get; }
        public object Value { get; }

        public DbType DBType => DataTypeConverter.GetDBType(Type);

        public QueryParameter(string name, string type, object value)
        {
            Name = name;
            Type = type;
            Value = value ?? DBNull.Value;
        }
    }
}
