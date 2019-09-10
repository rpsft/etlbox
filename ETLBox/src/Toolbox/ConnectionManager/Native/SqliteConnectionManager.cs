using System.Data;
using System.Data.SQLite;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager for an SQLite connection based on ADO.NET.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection =
    ///   new.SQLiteConnectionManager(new SQLiteConnectionString(
    ///     "Data Source=.\db\SQLite.db;Version=3;"));
    /// </code>
    /// </example>
    public class SQLiteConnectionManager : DbConnectionManager<SQLiteConnection, SQLiteCommand>
    {
        public SQLiteConnectionManager() : base() { }

        public SQLiteConnectionManager(SQLiteConnectionString connectionString) : base(connectionString) { }
        public SQLiteConnectionManager(string connectionString) : base(new SQLiteConnectionString(connectionString)) { }

        public override void BulkInsert(ITableData data, string tableName)
        {
            var connection = this.DbConnection as SQLiteConnection;
            var sourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            var sourceColumnValues = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => "?").ToList();
            var destColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();

            using (var transaction = connection.BeginTransaction())
            {
                while (data.Read())
                {
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText =
                        $@"INSERT INTO {tableName} 
({String.Join(",", sourceColumnNames)})
VALUES ({String.Join(",", sourceColumnValues)})
                ";
                        foreach (var mapping in destColumnNames)
                        {
                            SQLiteParameter par = new SQLiteParameter();
                            par.Value = data.GetValue(data.GetOrdinal(mapping));
                            command.Parameters.Add(par);
                        }

                        command.ExecuteNonQuery();
                    }
                }
                transaction.Commit();
            }
        }

        public override void BeforeBulkInsert() { }
        public override void AfterBulkInsert() { }

        public override IDbConnectionManager Clone()
        {
            SQLiteConnectionManager clone = new SQLiteConnectionManager((SQLiteConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
