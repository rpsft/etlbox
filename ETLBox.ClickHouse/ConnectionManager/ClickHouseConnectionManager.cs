using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ClickHouse.Ado;
using CsvHelper.Configuration;
using ETLBox.ClickHouse.ConnectionStrings;
using ETLBox.Primitives;

namespace ETLBox.ClickHouse.ConnectionManager
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

        private TableDefinition DestTableDef { get; set; } = null!;
        private Dictionary<string, TableColumn> DestinationColumns { get; set; } = null!;

        public override void BulkInsert(ITableData data, string tableName)
        {
            var csvData = new StringBuilder();
            var destColumnNames = data.GetColumnMapping()
                .Cast<IColumnMapping>()
                .Select(cm => cm.DataSetColumn)
                .ToList();

            while (data.Read())
            {
                var valSeparator = "";
                foreach (var destColumn in DestinationColumns.Keys)
                {
                    csvData.Append(valSeparator);
                    valSeparator = ",";
                    TableColumn colDef = DestinationColumns[destColumn];
                    object? val;
                    if (destColumnNames.Contains(colDef.Name))
                    {
                        var ordinal = data.GetOrdinal(destColumn);
                        val = data.GetValue(ordinal);
                    }
                    else
                    {
                        val = null;
                    }
                    csvData.Append(GetValue(val, colDef));
                }
                csvData.AppendLine();
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

        private string? GetValue(object? r, TableColumn col)
        {
            if (r == null)
            {
                return "";
            }
            if (r is DateTime && new[] { "DATE", "NULLABLE(DATE)" }.Contains(col.DataType.ToUpper()))
            {
                return $"{r:yyyy-MM-dd}";
            }
            if (r is DateTime)
            {
                return $"{r:yyyy-MM-dd HH:mm:ss}";
            }
            if (r is decimal or int or long or double or float)
            {
                return Convert.ToString(r, CultureInfo.InvariantCulture);
            }
            if (!DataTypeConverter.IsCharTypeDefinition(col.DataType) 
                && !col.DataType.ToUpper().Contains("STR"))
            {
                return ConvertToType(r, col.DataType);
            }
            if (r is bool)
            {
                return (bool)r ? "1" : "0";
            }
            var value = r?.ToString()!.Replace(@"""", @"""""");
            return $@"""{value}""";
        }

        private string? ConvertToType(object r, string dataType)
        {
            if (dataType.ToUpper().Contains("DECIMAL"))
            {
                return Convert.ToDecimal(r).ToString(CultureInfo.InvariantCulture);
            }
            if (dataType.ToUpper().Contains("INT"))
            {
                return Convert.ToInt64(r, CultureInfo.InvariantCulture).ToString();
            }
            if (dataType.ToUpper().Contains("DATETIME"))
            {
                return Convert.ToDateTime(r).ToString("yyyy-MM-dd HH:mm:ss");
            }
            if (dataType.ToUpper().Contains("DATE"))
            {
                return Convert.ToDateTime(r).ToString("yyyy-MM-dd");
            }
            if (dataType.ToUpper().Contains("BOOL") || dataType.ToUpper().Contains("BIT"))
            {
                return Convert.ToBoolean(r, CultureInfo.InvariantCulture).ToString();
            }
            else
            { 
                return r?.ToString();
            }
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

        public override bool IndexExists(ITask callingTask, string sql)
        {
            var res = new SqlTask(callingTask, sql).ExecuteScalar();
            return (!string.IsNullOrEmpty(res?.ToString()));
        }
    }
}
