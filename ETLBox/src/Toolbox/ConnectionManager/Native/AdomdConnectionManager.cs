using Microsoft.AnalysisServices.AdomdClient;
using System;

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
        public AdomdConnectionManager() : base() { }
        public AdomdConnectionManager(SqlConnectionString connectionString) : base(connectionString) { }
        public AdomdConnectionManager(string connectionString) : base(new SqlConnectionString(connectionString)) { }

        public override void BulkInsert(ITableData data, string tableName)
        {
            throw new NotImplementedException();
        }

        public override void BeforeBulkInsert(string tableName) { }
        public override void AfterBulkInsert(string tableName) { }

        public override IConnectionManager Clone()
        {
            if (LeaveOpen) return this;
            AdomdConnectionManager clone = new AdomdConnectionManager((SqlConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
