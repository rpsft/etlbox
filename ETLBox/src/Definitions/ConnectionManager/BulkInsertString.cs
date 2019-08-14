using System.Data;
using System.Data.Odbc;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System;

namespace ALE.ETLBox.ConnectionManager {
    /// <summary>
    /// This class creates the necessary sql statements that simulate the missing bulk insert function in Odbc connections.
    /// Normally this will be a insert into with multiple values. 
    /// For access databases this will differ. 
    /// </summary>
    /// <see cref="OdbcConnectionManager"/>
    /// <see cref="AccessOdbcConnectionManager"/>
    internal class BulkInsertString {
        internal bool IsAccessDatabase { get; set; }
        StringBuilder _sb { get; set; } = new StringBuilder();
        List<string> SourceColumnNames { get; set; }
        List<string> DestColumnNames { get; set; }

        internal string AccessDummyTableName {get;set; } 

        internal string CreateBulkInsertStatement(ITableData data, string tableName) {
            _sb.Clear();
            GetSourceAndDestColumNames(data);
            AppendBeginSql(tableName);
            while (data.Read()) {
                List<string> values = new List<string>();
                foreach (string destColumnName in DestColumnNames) {
                    int colIndex = data.GetOrdinal(destColumnName);
                    string dataTypeName = data.GetDataTypeName(colIndex);
                    if (data.IsDBNull(colIndex))
                        values.Add(NullValueSql(destColumnName));
                    else
                        values.Add(ValueSql(destColumnName,data.GetString(colIndex)));
                }
                AppendValueListSql(values, data.NextResult());
            }
            AppendEndSql();
            return _sb.ToString();
        }

        private void GetSourceAndDestColumNames(ITableData data) {
            SourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            DestColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
        }

        private void AppendBeginSql(string tableName) {
            _sb.AppendLine($@"insert into {tableName} ({string.Join(",", SourceColumnNames)})");
            if (IsAccessDatabase)
                _sb.AppendLine("  select * from (");
            else
                _sb.AppendLine("values");
        }

        private string ValueSql(string destColumnName, string data) =>
    IsAccessDatabase ? $"'{data}' as {destColumnName}" : $"'{data}'";

        private string NullValueSql(string destColumnName) =>
            IsAccessDatabase ? $"NULL as {destColumnName}" : "NULL";


        private void AppendValueListSql(List<string> values, bool lastItem) {
            if (IsAccessDatabase) {
                _sb.AppendLine("select " + string.Join(",", values) + $"  from {AccessDummyTableName} ");
                if (lastItem) _sb.AppendLine(" union all ");
            } else {
                _sb.Append("(" + string.Join(",", values) + $")");
                if (lastItem) _sb.AppendLine(",");
            }
        }

        private void AppendEndSql() {
            if (IsAccessDatabase)
                _sb.AppendLine(") a;");
        }
    }
}
