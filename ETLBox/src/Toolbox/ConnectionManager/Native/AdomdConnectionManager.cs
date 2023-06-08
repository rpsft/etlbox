using System.Globalization;
using Microsoft.AnalysisServices.AdomdClient;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager for Adomd connection to a sql server analysis server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.DefaultDbConnection = new AdmoConnectionManager(new ConnectionString("..connection string.."));
    /// </code>
    /// </example>
    public class AdomdConnectionManager : DbConnectionManager<AdomdConnection>
    {
        public override ConnectionManagerType ConnectionManagerType { get; } =
            ConnectionManagerType.Adomd;
        public override string QB { get; } = string.Empty;
        public override string QE { get; } = string.Empty;
        public override CultureInfo ConnectionCulture => CultureInfo.CurrentCulture;

        public AdomdConnectionManager() { }

        public AdomdConnectionManager(SqlConnectionString connectionString)
            : base(connectionString) { }

        public AdomdConnectionManager(string connectionString)
            : base(new SqlConnectionString(connectionString)) { }

        public override void BulkInsert(ITableData data, string tableName)
        {
            throw new NotImplementedException();
        }

        public override void PrepareBulkInsert(string tablename) { }

        public override void CleanUpBulkInsert(string tablename) { }

        public override void BeforeBulkInsert(string tableName) { }

        public override void AfterBulkInsert(string tableName) { }

        public override IConnectionManager Clone()
        {
            AdomdConnectionManager clone = new AdomdConnectionManager(
                (SqlConnectionString)ConnectionString
            )
            {
                MaxLoginAttempts = MaxLoginAttempts
            };
            return clone;
        }
    }
}
