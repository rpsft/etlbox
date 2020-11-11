using ETLBox.Connection;
using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace ETLBox.Helper
{
    /// <summary>
    /// This class creates the necessary sql statements that simulate the missing bulk insert function in various database or Odbc/OleDb connections.
    /// Normally this will be a insert into with multiple values, but depending on the database type this can be different.
    /// </summary>
    /// <typeparam name="T">ADO.NET database parameter type</typeparam>
    public class BulkInsertSql<T> where T : DbParameter, new()
    {
        #region Public properties
        /// <summary>
        /// Indicates that the values are stored in parameter objects.
        /// Default is true.
        /// </summary>
        public bool UseParameterQuery { get; set; } = true;

        /// <summary>
        /// Indicates that the parameter variables in the sql have a name
        /// </summary>
        public bool UseNamedParameters { get; set; }

        /// <summary>
        /// A list of parameters that contain the parameter objects for the generated sql query.
        /// Only has values if <see cref="UseParameterQuery"/> is true.
        /// </summary>
        public List<T> Parameters { get; internal set; }

        /// <summary>
        /// When creating a bulk insert sql statement for Access, a dummy table is needed.
        /// The name of the dummy table is specified here.
        /// </summary>
        public string AccessDummyTableName { get; set; }

        /// <summary>
        /// The type of the database that the bulk insert statement is designed for
        /// </summary>
        public ConnectionManagerType ConnectionType { get; set; }

        /// <summary>
        /// The quotatation begin character that the database uses. (E.g. '[' for SqlServer or '"' for Postgres)
        /// </summary>
        public string QB { get; set; }

        /// <summary>
        /// The quotatation end character that the database uses. (E.g. ']' for SqlServer or '"' for Postgres)
        /// </summary>
        public string QE { get; set; }

        /// <summary>
        /// The formatted table name of the destination table
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        /// <summary>
        /// If set to true, the parameter list will contain not only the object value, but also
        /// the DbType and Size of the parameter. This should only be necessary for SqlServer. 
        /// Default is false.
        /// </summary>
        public bool AddDbTypesFromDefinition { get; set; }

        #endregion

        #region Implementation

        string TableName;
        int ParameterNameCount;
        string ParameterPlaceholder => ConnectionType == ConnectionManagerType.Oracle ? ":" : "@";
        bool IsAccessDatabase => ConnectionType == ConnectionManagerType.Access;
        StringBuilder QueryText;
        List<string> SourceColumnNames;
        List<string> DestColumnNames;
        ITableData TableData;
        /// <summary>
        /// Create the sql that can be used as a bulk insert.
        /// </summary>
        /// <param name="data">The data that should be inserted into the destination table</param>
        /// <param name="tableName">The name of the destination table</param>
        /// <returns></returns>
        public string CreateBulkInsertStatement(ITableData data, string tableName)
        {
            InitObjects();
            TableName = tableName;
            TableData = data;
            GetSourceAndDestColumnNames();
            AppendBeginSql();
            ReadDataAndCreateQuery();
            AppendEndSql();
            return QueryText.ToString();
        }

        private void InitObjects()
        {
            QueryText = new StringBuilder();
            Parameters = new List<T>();
        }

        private void GetSourceAndDestColumnNames()
        {
            SourceColumnNames = TableData.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            DestColumnNames = TableData.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
        }

        private void ReadDataAndCreateQuery()
        {
            while (TableData.Read())
            {
                List<string> values = new List<string>();
                foreach (string destColumnName in DestColumnNames)
                {
                    int colIndex = TableData.GetOrdinal(destColumnName);
                    if (TableData.IsDBNull(colIndex))
                        AddNullValue(values, destColumnName, colIndex);
                    else
                        AddNonNullValue(values, destColumnName, colIndex);
                }
                AppendValueListSql(values, TableData.NextResult());
            }
        }

        private void AddNullValue(List<string> values, string destColumnName, int colIndex)
        {
            if (UseParameterQuery && ConnectionType != ConnectionManagerType.Oracle)
            {
                values.Add(CreateParameterWithValue(DBNull.Value, colIndex));
            }
            else
            {
                string value = IsAccessDatabase ? $"NULL AS {destColumnName}" : "NULL";
                values.Add(value);
            }

        }

        private void AddNonNullValue(List<string> values, string destColumnName, int colIndex)
        {
            if (UseParameterQuery)
            {
                values.Add(CreateParameterWithValue(TableData.GetValue(colIndex), colIndex));
            }
            else
            {
                string value = TableData.GetString(colIndex).Replace("'", "''");
                string valueSql = IsAccessDatabase ? $"'{value}' AS {destColumnName}"
                    : $"'{value}'";
                values.Add(valueSql);
            }

        }

        private string CreateParameterWithValue(object parValue, int colIndex)
        {
            var par = new T();
            if (ConnectionType == ConnectionManagerType.Oracle && parValue is Enum) //Enums don't work obviously
                par.Value = (int)parValue;
            par.Value = parValue;

            if (AddDbTypesFromDefinition)
            {
                var dbtypestring = TableData.GetDataTypeName(colIndex);
                par.DbType = DataTypeConverter.GetDBType(dbtypestring);
                par.Size = DataTypeConverter.GetStringLengthFromCharString(dbtypestring);
            }

            Parameters.Add(par);
            if (UseNamedParameters)
            {
                string parName = $"{ParameterPlaceholder}P{ParameterNameCount++}";
                par.ParameterName = parName;
                return parName;
            }
            else
            {
                return "?";
            }
        }

        private void AppendBeginSql()
        {
            QueryText.AppendLine($@"INSERT INTO {TN.QuotatedFullName} ({string.Join(",", SourceColumnNames.Select(col => QB + col + QE))})");
            if (IsAccessDatabase)
                QueryText.AppendLine("  SELECT * FROM (");
            else if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine($" SELECT {string.Join(",", SourceColumnNames.Select(col => QB + col + QE))} FROM (");
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
            else if (ConnectionType == ConnectionManagerType.Oracle)
            {
                QueryText.Append("SELECT ");
                for (int i = 0; i < values.Count; i++)
                {
                    QueryText.Append($"{values[i]} {QB}{DestColumnNames[i]}{QE}");
                    if (i + 1 < values.Count)
                        QueryText.Append(",");
                }
                QueryText.AppendLine(" FROM DUAL");
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
            else if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine(")");
        }

        #endregion
    }
}
