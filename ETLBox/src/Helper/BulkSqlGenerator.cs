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
    public class BulkSqlGenerator<T> where T : DbParameter, new()
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
        /// Indicates if the VALUES table descriptor needs the keyword ROW (MySql only)
        /// </summary>
        public bool IsMariaDb { get; set; }

        /// <summary>
        /// If set to true, the parameter list will contain not only the object value, but also
        /// the DbType and Size of the parameter. This should only be necessary for SqlServer. 
        /// Default is false.
        /// </summary>
        public bool AddDbTypesFromDefinition { get; set; }

        /// <summary>
        /// If set to true, the values for the parameters are tried to convert into the corresponding .NET 
        /// data type that is suitable for the corresponding database column. If a database column is of type INTEGER,
        /// but the input data is a string like "7", then the parameter value is converted into an System.Int32.
        /// The most ADO.NET connectors do this automatically, but this can be useful for Postgres. 
        /// Only works if <see cref="AddDbTypesFromDefinition"/> is set to true.
        /// </summary>
        public bool TryConvertParameterData { get; set; }

        /// <summary>
        /// The destination table name
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// The data used for the bulk operation
        /// </summary>
        public ITableData TableData { get; set; }

        public ICollection<string> UpdateSetColumnNames { get; set; } = new List<string>();
        public ICollection<string> UpdateJoinColumnNames { get; set; } = new List<string>();

        #endregion

        public BulkSqlGenerator()
        {

        }

        public BulkSqlGenerator(ITableData data, string tableName)
        {
            TableName = tableName;
            TableData = data;
        }

        #region Implementation


        int ParameterNameCount;
        string ParameterPlaceholder => ConnectionType == ConnectionManagerType.Oracle ? ":" : "@";
        bool IsAccessDatabase => ConnectionType == ConnectionManagerType.Access;
        StringBuilder QueryText;
        List<string> SourceColumnNames;
        List<string> DestColumnNames;
        ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        /// <summary>
        /// Create the sql that can be used as a bulk insert.
        /// </summary>
        /// <param name="data">The data that should be inserted into the destination table</param>
        /// <param name="tableName">The name of the destination table</param>
        /// <returns></returns>
        public string CreateBulkInsertStatement()
        {
            InitObjects();
            BeginInsertSql();
            CreateValueSqlFromData();
            EndInsertSql();
            return QueryText.ToString();
        }

        public string CreateBulkDeleteStatement()
        {
            if (IsAccessDatabase) throw new Exception("Bulk delete is currently not supported for Access!");
            InitObjects();
            BeginDeleteSql();
            CreateValueSqlFromData();
            EndDeleteSql();
            return QueryText.ToString();
        }

        public string CreateBulkUpdateStatement()
        {
            if (IsAccessDatabase) throw new Exception("Bulk update is currently not supported for Access!");
            InitObjects();
            BeginUpdateSql();
            CreateValueSqlFromData();
            EndUpdateSql();
            return QueryText.ToString();
        }


        private void InitObjects()
        {
            QueryText = new StringBuilder();
            Parameters = new List<T>();
            ParameterNameCount = 0;
            SourceColumnNames = TableData.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.SourceColumn).ToList();
            DestColumnNames = TableData.ColumnMapping.Cast<IColumnMapping>().Select(cm => cm.DataSetColumn).ToList();
        }

        private void BeginInsertSql()
        {
            QueryText.AppendLine($@"INSERT INTO {TN.QuotatedFullName} ({string.Join(",", SourceColumnNames.Select(col => QB + col + QE))})");
            if (IsAccessDatabase)
                QueryText.AppendLine("  SELECT * FROM (");
            else if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine($" SELECT {string.Join(",", SourceColumnNames.Select(col => QB + col + QE))} FROM (");
            else
                QueryText.AppendLine("VALUES");
        }

        private void BeginDeleteSql()
        {
            if (ConnectionType == ConnectionManagerType.Oracle)
            {
                QueryText.AppendLine($@"DELETE FROM {TN.QuotatedFullName} dt WHERE EXISTS ( ");
                QueryText.AppendLine($" SELECT {string.Join(",", SourceColumnNames.Select(col => QB + col + QE))} FROM (");
            }
            else if (ConnectionType == ConnectionManagerType.Postgres)
            {
                QueryText.AppendLine($@"DELETE FROM {TN.QuotatedFullName} dt");
                QueryText.AppendLine($"USING ( VALUES");
            }
            else
            {
                QueryText.AppendLine($@"DELETE dt FROM {TN.QuotatedFullName} dt");
                QueryText.AppendLine("INNER JOIN (");
                if (ConnectionType == ConnectionManagerType.MySql && IsMariaDb)
                    //https://dba.stackexchange.com/questions/177312/does-mariadb-or-mysql-implement-the-values-expression-table-value-constructor
                    //QueryText.AppendLine($"VALUES ( {string.Join(",", SourceColumnNames.Select(col => $"'{col}'"))}  ),");
                    QueryText.AppendLine($@"WITH ws ( {string.Join(", ", SourceColumnNames.Select(col => $"{QB}{col}{QE}"))} ) AS ( VALUES");
                else
                    QueryText.AppendLine("VALUES");
            }
        }

        private void BeginUpdateSql()
        {
            if (ConnectionType == ConnectionManagerType.MySql)
            {
                QueryText.AppendLine($@"UPDATE {TN.QuotatedFullName} ut");
                QueryText.AppendLine("INNER JOIN (");
                if (IsMariaDb)
                    QueryText.AppendLine($@"WITH ws ( {string.Join(", ", SourceColumnNames.Select(col => $"{QB}{col}{QE}"))} ) AS ( VALUES");
                else
                    QueryText.AppendLine("VALUES");
            }
            else if (ConnectionType == ConnectionManagerType.Postgres)
            {
                QueryText.AppendLine($@"UPDATE {TN.QuotatedFullName} ut");
                QueryText.AppendLine($@"SET {string.Join(", ", UpdateSetColumnNames.Select(col => $"{QB}{col}{QE} = vt.{QB}{col}{QE}"))}");
                QueryText.AppendLine($@"FROM (");
                QueryText.AppendLine("VALUES");
            }
            //https://www.orafaq.com/node/2450
            else if (ConnectionType == ConnectionManagerType.Oracle)
            {
                QueryText.AppendLine($@"MERGE INTO {TN.QuotatedFullName} ut");
                QueryText.AppendLine($@"USING (");
            }
            else
            {
                QueryText.AppendLine($@"UPDATE ut");
                QueryText.AppendLine($@"SET {string.Join(", ", UpdateSetColumnNames.Select(col => $"ut.{QB}{col}{QE} = vt.{QB}{col}{QE}"))}");
                QueryText.AppendLine($@"FROM {TN.QuotatedFullName} ut");
                QueryText.AppendLine("INNER JOIN (");
                QueryText.AppendLine("VALUES");
            }
        }


        private void CreateValueSqlFromData()
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
            else
                par.Value = parValue;

            if (AddDbTypesFromDefinition)
            {
                var dbtypestring = TableData.GetDataTypeName(colIndex);
                par.DbType = DataTypeConverter.GetDBType(dbtypestring);
                par.Size = DataTypeConverter.GetStringLengthFromCharString(dbtypestring);
                if (TryConvertParameterData)
                    TryConvertParameter(parValue, par, dbtypestring);
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

        private static void TryConvertParameter(object parValue, T par, string dbtypestring)
        {
            if (parValue == DBNull.Value) return;
            try
            {
                par.Value = Convert.ChangeType(parValue, DataTypeConverter.GetTypeObject(dbtypestring));
            }
            catch
            {

            }
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
            else if (ConnectionType == ConnectionManagerType.MySql && !IsMariaDb)
            {
                QueryText.Append("ROW(" + string.Join(",", values) + $")");
                if (lastItem) QueryText.AppendLine(",");
            }
            else
            {
                QueryText.Append("(" + string.Join(",", values) + $")");
                if (lastItem) QueryText.AppendLine(",");
            }
        }

        private void EndInsertSql()
        {
            if (IsAccessDatabase)
                QueryText.AppendLine(") a;");
            else if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine(")");
        }

        private void EndDeleteSql()
        {
            QueryText.AppendLine(Environment.NewLine + ")");
            if (ConnectionType == ConnectionManagerType.MySql && IsMariaDb)
                QueryText.AppendLine($" SELECT * FROM ws ) vt");
            else if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine($"vt");
            else
                QueryText.AppendLine($"vt ( { string.Join(",", SourceColumnNames.Select(col => $"{QB}{col}{QE}"))} )");
            if (ConnectionType == ConnectionManagerType.Oracle || ConnectionType == ConnectionManagerType.Postgres)
                QueryText.Append(" WHERE ");
            else
                QueryText.Append(" ON ");
            QueryText.AppendLine(string.Join(Environment.NewLine + " AND ",
                    SourceColumnNames.Select(col => $@"( ( dt.{QB}{col}{QE} = vt.{QB}{col}{QE} ) OR ( dt.{QB}{col}{QE} IS NULL AND vt.{QB}{col}{QE} IS NULL) )")));
            if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine(")");
        }

        private void EndUpdateSql()
        {            
            //https://www.orafaq.com/node/2450
            if (ConnectionType == ConnectionManagerType.Oracle)
            {
                QueryText.AppendLine(") vt ");
                QueryText.Append(" ON ");
                QueryText.AppendLine(string.Join(Environment.NewLine + " AND ",
                        UpdateJoinColumnNames.Select(col => $@"( ( ut.{QB}{col}{QE} = vt.{QB}{col}{QE} ) OR ( ut.{QB}{col}{QE} IS NULL AND vt.{QB}{col}{QE} IS NULL) )")));
                QueryText.AppendLine(" WHEN MATCHED THEN UPDATE ");
                QueryText.AppendLine($@"SET {string.Join(", ", UpdateSetColumnNames.Select(col => $"ut.{QB}{col}{QE} = vt.{QB}{col}{QE}"))}");
            }
            else if (ConnectionType == ConnectionManagerType.MySql)
            {
                if (IsMariaDb)
                    QueryText.AppendLine($" ) SELECT * FROM ws ) vt");
                else
                    QueryText.AppendLine($") vt ( { string.Join(",", SourceColumnNames.Select(col => $"{QB}{col}{QE}"))} )");
                QueryText.Append(" ON ");
                QueryText.AppendLine(string.Join(Environment.NewLine + " AND ",
        UpdateJoinColumnNames.Select(col => $@"( ( ut.{QB}{col}{QE} = vt.{QB}{col}{QE} ) OR ( ut.{QB}{col}{QE} IS NULL AND vt.{QB}{col}{QE} IS NULL) )")));
                QueryText.AppendLine($@"SET {string.Join(", ", UpdateSetColumnNames.Select(col => $"ut.{QB}{col}{QE} = vt.{QB}{col}{QE}"))}");
            }
            else if (ConnectionType == ConnectionManagerType.Postgres)
            {
                QueryText.AppendLine($") vt ( { string.Join(",", SourceColumnNames.Select(col => $"{QB}{col}{QE}"))} )");
                QueryText.Append(" WHERE ");
                QueryText.AppendLine(string.Join(Environment.NewLine + " AND ",
                        UpdateJoinColumnNames.Select(col => $@"( ( ut.{QB}{col}{QE} = vt.{QB}{col}{QE} ) OR ( ut.{QB}{col}{QE} IS NULL AND vt.{QB}{col}{QE} IS NULL) )")));
            }
            else
            {
                QueryText.AppendLine($") vt ( { string.Join(",", SourceColumnNames.Select(col => $"{QB}{col}{QE}"))} )");
                QueryText.Append(" ON ");
                QueryText.AppendLine(string.Join(Environment.NewLine + " AND ",
                        UpdateJoinColumnNames.Select(col => $@"( ( ut.{QB}{col}{QE} = vt.{QB}{col}{QE} ) OR ( ut.{QB}{col}{QE} IS NULL AND vt.{QB}{col}{QE} IS NULL) )")));
            }
        }

        #endregion
    }
}
