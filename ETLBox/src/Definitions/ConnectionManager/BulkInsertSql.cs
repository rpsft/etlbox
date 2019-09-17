using System.Data;
using System.Data.Odbc;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System;
using System.Data.Common;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// This class creates the necessary sql statements that simulate the missing bulk insert function in Odbc connections.
    /// Normally this will be a insert into with multiple values.
    /// For access databases this will differ.
    /// </summary>
    /// <see cref="OdbcConnectionManager"/>
    /// <see cref="AccessOdbcConnectionManager"/>
    internal class BulkInsertSql<T> where T: DbParameter, new()
    {
        internal bool IsAccessDatabase { get; set; }
        internal bool UseParameterQuery { get; set; } = true;
        internal List<T> Parameters { get; set; }
        StringBuilder QueryText { get; set; }
        List<string> SourceColumnNames { get; set; }
        List<string> DestColumnNames { get; set; }
        internal string AccessDummyTableName { get; set; }


        internal string CreateBulkInsertStatement(ITableData data, string tableName)
        {
            InitObjects();
            GetSourceAndDestColumNames(data);
            AppendBeginSql(tableName);
            ReadDataAndCreateQuery(data);
            AppendEndSql();
            return QueryText.ToString();
        }

        private void InitObjects()
        {
            QueryText = new StringBuilder();
            Parameters = new List<T>();
        }

        private void GetSourceAndDestColumNames(ITableData data)
        {
            SourceColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            DestColumnNames = data.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
        }

        private void ReadDataAndCreateQuery(ITableData data)
        {
            while (data.Read())
            {
                List<string> values = new List<string>();
                foreach (string destColumnName in DestColumnNames)
                {
                    int colIndex = data.GetOrdinal(destColumnName);
                    if (data.IsDBNull(colIndex))
                        AddNullValue(values, destColumnName);
                    else
                        AddNonNullValue(data, values, destColumnName, colIndex);
                }
                AppendValueListSql(values, data.NextResult());
            }
        }

        private void AddNullValue(List<string> values, string destColumnName)
        {
            if (UseParameterQuery)
            {
                var par = new T();
                //par.ParameterName = destColumnName;
                par.Value = DBNull.Value;
                Parameters.Add(par);
                //Parameters.Add(new OdbcParameter(destColumnName, DBNull.Value));
                values.Add("?");
            }
            else
            {
                string value = IsAccessDatabase ? $"NULL AS {destColumnName}" : "NULL";
                values.Add(value);
            }

        }

        private void AddNonNullValue(ITableData data, List<string> values, string destColumnName, int colIndex)
        {
            if (UseParameterQuery)
            {
                var par = new T();
                //par.ParameterName = destColumnName;
                par.Value = data.GetValue(colIndex);
                Parameters.Add(par);
                //Parameters.Add(new OdbcParameter(destColumnName, data.GetValue(colIndex)));
                values.Add("?");
            }
            else
            {
                string value = data.GetString(colIndex).Replace("'", "''");
                string valueSql = IsAccessDatabase ? $"'{value}' AS {destColumnName}"
                    : $"'{value}'";
                values.Add(valueSql);
            }

        }

        private void AppendBeginSql(string tableName)
        {
            QueryText.AppendLine($@"INSERT INTO {tableName} ({string.Join(",", SourceColumnNames)})");
            if (IsAccessDatabase)
                QueryText.AppendLine("  SELECT * FROM (");
            else
                QueryText.AppendLine("VALUES");
        }

        private void AppendValueListSql(List<string> values, bool lastItem)
        {
            if (IsAccessDatabase)
            {
                QueryText.AppendLine("SELECT " + string.Join(",", values) + $"  FROM {AccessDummyTableName} ");
                if (lastItem) QueryText.AppendLine(" UNION ALL ");
            }
            else
            {
                QueryText.Append("(" + string.Join(",", values) + $")");
                if (lastItem) QueryText.AppendLine(",");
            }
        }

        private void AppendEndSql()
        {
            if (IsAccessDatabase)
                QueryText.AppendLine(") a;");
        }


        internal string CreateBulkInsertStatementWithParameter(ITableData data, string tableName, ref List<OdbcParameter> parameters)
        {
            QueryText.Clear();
            GetSourceAndDestColumNames(data);
            AppendBeginSql(tableName);
            while (data.Read())
            {
                QueryText.Append("(");
                string[] placeholder = new string[DestColumnNames.Count];
                QueryText.Append(string.Join(",", placeholder.Select(s => s + "?")));
                QueryText.AppendLine(")");
                foreach (string destColumnName in DestColumnNames)
                {
                    int colIndex = data.GetOrdinal(destColumnName);
                    string dataTypeName = data.GetDataTypeName(colIndex);
                    if (data.IsDBNull(colIndex))
                        parameters.Add(new OdbcParameter(destColumnName, DBNull.Value));
                    else
                        parameters.Add(new OdbcParameter(destColumnName, data.GetValue(colIndex)));
                }
                if (data.NextResult())
                    QueryText.Append(",");
            }
            AppendEndSql();
            return QueryText.ToString();
        }
    }

    internal class BulkInsertSql : BulkInsertSql<OdbcParameter>
    {

    }
}
