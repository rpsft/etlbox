using Microsoft.AnalysisServices.AdomdClient;
using System;
using System.Data;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager for Adomd connection to a sql server analysis server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = new AdmoConnectionManager(new ConnectionString("..connection string.."));
    /// </code>
    /// </example>
    public class AdomdConnectionManager : DbConnectionManager<AdomdConnection>
    {

        public AdomdConnectionManager() : base() { }

        public AdomdConnectionManager(ConnectionString connectionString) : base(connectionString) { }
        public AdomdConnectionManager(string connectionString) : base(new ConnectionString(connectionString)) { }

        public override void BulkInsert(ITableData data, string tableName)
        {
            throw new NotImplementedException();
        }

        public override void BeforeBulkInsert(string tableName) { }
        public override void AfterBulkInsert(string tableName) { }

        public override IConnectionManager Clone()
        {
            AdomdConnectionManager clone = new AdomdConnectionManager((ConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;

        }

    }
}
