using System;
using System.Data;
using System.Globalization;
using System.Linq;
using Microsoft.Data.Sqlite;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager for an SQLite connection based on ADO.NET.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.DefaultDbConnection =
    ///   new.SQLiteConnectionManager(new SQLiteConnectionString(
    ///     "Data Source=.\db\SQLite.db;Version=3;"));
    /// </code>
    /// </example>
    public class SQLiteConnectionManager : DbConnectionManager<SqliteConnection>
    {
        public override ConnectionManagerType ConnectionManagerType { get; } = ConnectionManagerType.SQLite;
        public override string QB { get; } = @"""";
        public override string QE { get; } = @"""";
        public override CultureInfo ConnectionCulture => CultureInfo.InvariantCulture;
        public override bool SupportDatabases { get; } = false;
        public override bool SupportProcedures { get; } = false;
        public override bool SupportSchemas { get; } = false;
        public override bool SupportComputedColumns { get; } = false;

        public bool ModifyDBSettings { get; set; } = false;

        public SQLiteConnectionManager() : base()
        {
        }

        public SQLiteConnectionManager(SQLiteConnectionString connectionString) : base(connectionString)
        {
        }

        public SQLiteConnectionManager(string connectionString) : base(new SQLiteConnectionString(connectionString))
        {
        }

        string Synchronous { get; set; }
        string JournalMode { get; set; }

        public override void BulkInsert(ITableData data, string tableName)
        {
            var sourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            var paramNames = data.ColumnMapping.Cast<IColumnMapping>().Select((_, i) => $"$p{i}").ToArray();
            var destColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();

            var existingTransaction = Transaction as SqliteTransaction;
            SqliteTransaction bulkTransaction = null;
            if (existingTransaction == null)
                bulkTransaction = this.DbConnection.BeginTransaction();
            using (bulkTransaction)
            using (var command = this.DbConnection.CreateCommand())
            {
                command.Transaction = existingTransaction ?? bulkTransaction;
                command.CommandText =
                    $@"INSERT INTO {tableName} 
({string.Join(",", sourceColumnNames)})
VALUES ({string.Join(",", paramNames)})";
                command.Prepare();
                while (data.Read())
                {
                    command.Parameters.AddRange(
                        destColumnNames.Select((n, i) => new SqliteParameter
                        {
                            ParameterName = paramNames[i],
                            Value = data.GetValue(data.GetOrdinal(n))
                        }));
                    command.ExecuteNonQuery();
                    command.Parameters.Clear();
                }

                bulkTransaction?.Commit();
            }
        }

        public override void PrepareBulkInsert(string tablename)
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

        public override void CleanUpBulkInsert(string tablename)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    this.ExecuteNonQuery($"PRAGMA synchronous = {Synchronous}");
                    this.ExecuteNonQuery($"PRAGMA journal_mode = {JournalMode}");
                }
                catch
                {
                }
            }
        }

        public override void BeforeBulkInsert(string tableName)
        {
        }

        public override void AfterBulkInsert(string tableName)
        {
        }

        public override IConnectionManager Clone()
        {
            var clone = new SQLiteConnectionManager((SQLiteConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts,
                ModifyDBSettings = this.ModifyDBSettings
            };
            return clone;
        }
    }
}