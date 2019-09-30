using ALE.ETLBox.Helper;
using CsvHelper;
using MySql.Data.MySqlClient;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager of a classic ADO.NET connection to a (Microsoft) Sql Server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;"));
    /// </code>
    /// </example>
    public class PostgresConnectionManager : DbConnectionManager<NpgsqlConnection>
    {
        public bool ModifyDBSettings { get; set; } = true;

        public PostgresConnectionManager() : base() { }

        public PostgresConnectionManager(PostgresConnectionString connectionString) : base(connectionString) { }

        public PostgresConnectionManager(string connectionString) : base(new PostgresConnectionString(connectionString)) { }

        string PageVerify { get; set; }
        string RecoveryModel { get; set; }

        //BulkInsertSql<NpgsqlParameter> bulkInsert = new BulkInsertSql<NpgsqlParameter>()
        //{
        //    UseParameterQuery = true,
        //    UseNamedParameters = true,
        //    ConnectionType = ConnectionManagerType.Postgres
        //};
        //string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
        //var cmd = DbConnection.CreateCommand();
        //cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
        //cmd.CommandText = sql;
        //cmd.Prepare();
        //cmd.ExecuteNonQuery();
        public override void BulkInsert(ITableData data, string tableName)
        {
            var TN = new TableNameDescriptor(tableName, ConnectionManagerType.Postgres);
            var sourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            var destColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
            var quotedDestColumns = destColumnNames.Select(col => TN.QB + col + TN.QE);

            TableDefinition DestTableDef = TableDefinition.GetDefinitionFromTableName(tableName, this.Clone());
            using (var writer = DbConnection.BeginBinaryImport($@"
COPY {TN.QuotatedFullName} ({string.Join(", ", quotedDestColumns)})
FROM STDIN (FORMAT BINARY)"))
            {
                while (data.Read())
                {
                    writer.StartRow();
                    foreach (var destCol in destColumnNames)
                    {
                        var colDef = DestTableDef.Columns.Where(col => col.Name == destCol).FirstOrDefault();
                        int ordinal = data.GetOrdinal(destCol);
                        object val = data.GetValue(ordinal);
                        if (val != null)
                        {
                            object convertedVal = System.Convert.ChangeType(data.GetValue(ordinal),colDef.NETDataType);
                            writer.Write(convertedVal, colDef.DataType);

                        }
                        else
                        {
                            writer.WriteNull();
                        }
                    }
                }
                writer.Complete();
            }
        }

        public override void BeforeBulkInsert()
        {
            if (ModifyDBSettings)
            {
                ;
            }
        }

        public override void AfterBulkInsert()
        {
            if (ModifyDBSettings)
            {
                ;
            }
        }

        public override IConnectionManager Clone()
        {
            PostgresConnectionManager clone = new PostgresConnectionManager((PostgresConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
