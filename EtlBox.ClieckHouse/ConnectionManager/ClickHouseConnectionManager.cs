using System.Collections.Generic;
using System.Data;
using System.Formats.Asn1;
using System.Globalization;
using System.IO;
using System.Text;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ClickHouse.Ado;
using CsvHelper;
using CsvHelper.Configuration;
using EtlBox.ClickHouse.ConnectionStrings;

namespace EtlBox.ClickHouse.ConnectionManager
{
    public class ClickHouseConnectionManager : DbConnectionManager<ClickHouseConnection>
    {
        public override ConnectionManagerType ConnectionManagerType { get; } =
                    ConnectionManagerType.ClickHouse;
        public override string QB { get; } = @"`";
        public override string QE { get; } = @"`";
        public override CultureInfo ConnectionCulture => CultureInfo.CurrentCulture;
        public CsvConfiguration Configuration { get; set; }

        public ClickHouseConnectionManager()
        {
            Configuration = new CsvConfiguration(CultureInfo.InvariantCulture);
        }

        public ClickHouseConnectionManager(ClickHouseConnectionString connectionString)
            : base(connectionString)
        {
            Configuration = new CsvConfiguration(CultureInfo.InvariantCulture);
        }

        public ClickHouseConnectionManager(string connectionString)
            : base(new ClickHouseConnectionString(connectionString))
        {
            Configuration = new CsvConfiguration(CultureInfo.InvariantCulture);
        }

        private TableDefinition DestTableDef { get; set; }
        private Dictionary<string, TableColumn> DestinationColumns { get; set; }

        public override void BulkInsert(ITableData data, string tableName)
        {
            var csvData = new StringBuilder();
            foreach (var row in data.Rows)
            {
                var rowData = string.Join(Configuration.Delimiter, row);
                csvData.AppendLine(rowData);
            }

            if (DbConnection.State != ConnectionState.Open)
            {
                DbConnection.Open();
            }
            using var cmd = DbConnection.CreateCommand();
            cmd.CommandText = $@"
            INSERT INTO {QB}{tableName}{QE}
            FORMAT CSV
            {csvData}";

            cmd.ExecuteNonQuery();
        }

        public override void PrepareBulkInsert(string tableName)
        {
            ReadTableDefinition(tableName);
        }

        private void ReadTableDefinition(string tablename)
        {
            DestTableDef = TableDefinition.GetDefinitionFromTableName(this, tablename);
            DestinationColumns = new Dictionary<string, TableColumn>();
            foreach (var colDef in DestTableDef.Columns)
            {
                DestinationColumns.Add(colDef.Name, colDef);
            }
        }

        public override void CleanUpBulkInsert(string tableName) { }

        public override void BeforeBulkInsert(string tableName)
        {
            if (DestinationColumns == null)
                ReadTableDefinition(tableName);
        }

        public override void AfterBulkInsert(string tableName) { }

        public override IConnectionManager Clone()
        {
            var clone = new ClickHouseConnectionManager(
                (ClickHouseConnectionString)ConnectionString
            )
            {
                MaxLoginAttempts = MaxLoginAttempts
            };
            return clone;
        }

        protected override void MapQueryParameterToCommandParameter(
            QueryParameter source,
            IDbDataParameter destination
        )
        {
            destination.ParameterName = source.Name;
            destination.DbType = source.DBType;
            destination.Value = source.Value;
        }
    }
}
