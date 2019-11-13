using System;
using System.Data;
using System.Data.SQLite;
using System.Linq;

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
    public class SQLiteConnectionManager : DbConnectionManager<SQLiteConnection>
    {
        public bool ModifyDBSettings { get; set; } = false;

        public SQLiteConnectionManager() : base() { }
        public SQLiteConnectionManager(SQLiteConnectionString connectionString) : base(connectionString) { }
        public SQLiteConnectionManager(string connectionString) : base(new SQLiteConnectionString(connectionString)) { }

        string Synchronous { get; set; }
        string JournalMode { get; set; }

        public override void BulkInsert(ITableData data, string tableName)
        {
            var sourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            var sourceColumnValues = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => "?").ToList();
            var destColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();

            using (var transaction = this.DbConnection.BeginTransaction())
            using (var command = this.DbConnection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText =
                $@"INSERT INTO {tableName} 
({String.Join(",", sourceColumnNames)})
VALUES ({String.Join(",", sourceColumnValues)})
                ";
                command.Prepare();
                while (data.Read())
                {
                    foreach (var mapping in destColumnNames)
                    {
                        SQLiteParameter par = new SQLiteParameter();
                        par.Value = data.GetValue(data.GetOrdinal(mapping));
                        command.Parameters.Add(par);
                    }
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }
                transaction.Commit();
            }
        }


        public override void BeforeBulkInsert(string tableName)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    Synchronous = this.ExecuteScalar("PRAGMA synchronous").ToString();
                    JournalMode = this.ExecuteScalar("PRAGMA journal_mode").ToString();
                    this.ExecuteNonQuery("PRAGMA synchronous = OFF");
                    this.ExecuteNonQuery("PRAGMA journal_mode = MEMORY");
                }
                catch
                {
                    ModifyDBSettings = false;
                }
            }
        }

        public override void AfterBulkInsert(string tableName)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    this.ExecuteNonQuery($"PRAGMA synchronous = {Synchronous}");
                    this.ExecuteNonQuery($"PRAGMA journal_mode = {JournalMode}");
                }
                catch { }
            }
        }

        public override IConnectionManager Clone()
        {
            if (LeaveOpen) return this;
            SQLiteConnectionManager clone = new SQLiteConnectionManager((SQLiteConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
