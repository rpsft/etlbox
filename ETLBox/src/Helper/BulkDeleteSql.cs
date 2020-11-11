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
    /// This class converts a list of data into a table value constructor syntax (e.g. VALUES ( ))
    /// Normally this will be something like VALUES ('..') AS T(v), but this
    /// depeneds on the database types.
    /// </summary>
    /// <typeparam name="T">ADO.NET database parameter type</typeparam>
    public class BulkDeleteSql<T> where T : DbParameter, new()
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
        public List<T> Parameters { get; internal set; } = new List<T>();

        /// <summary>
        /// The type of the database that the bulk insert statement is designed for
        /// </summary>
        public ConnectionManagerType ConnectionType { get; set; }

        /// <summary>
        /// The quotation begin character that the database uses. (E.g. '[' for SqlServer or '"' for Postgres)
        /// </summary>
        public string QB { get; set; }

        /// <summary>
        /// The quotation end character that the database uses. (E.g. ']' for SqlServer or '"' for Postgres)
        /// </summary>
        public string QE { get; set; }

        /// <summary>
        /// The formatted table name of the destination table
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        #endregion

        public BulkDeleteSql()
        {

        }

        #region Implementation

        string TableName;
        int ParameterNameCount;
        string ParameterPlaceholder => ConnectionType == ConnectionManagerType.Oracle ? ":" : "@";
        StringBuilder QueryText = new StringBuilder();
        List<string> ColumnNames;        

        /// <summary>
        /// Create the sql that can be used as a bulk insert.
        /// </summary>
        /// <param name="data">The data that should be inserted into the destination table</param>
        /// <param name="tableName">The name of the destination table</param>
        /// <returns></returns>
        public string CreateBulkDeleteStatement(List<string> columnNames, string tableName, List<object[]> rows)
        {
            TableName = tableName;
            ColumnNames = columnNames.ToList();
            AppendBeginSql();
            ReadDataAndCreateQuery(rows);
            AppendEndSql();
            return QueryText.ToString();
        }

        private void ReadDataAndCreateQuery(List<object[]> rows)
        {
            foreach (var row in rows)
            {
                int colIndex = 0;
                List<string> values = new List<string>();                
                foreach (string columnName in ColumnNames)
                {
                    if (row[colIndex] == null)
                        AddNullValue(values, columnName);
                    else
                        AddNonNullValue(row[colIndex], values, columnName);
                    colIndex++;
                }
                AppendValueListSql(values, colIndex == row.Length);
            }
        }

        private void AddNullValue(List<string> values, string destColumnName)
        {
            if (UseParameterQuery && ConnectionType != ConnectionManagerType.Oracle)
                values.Add(CreateParameterWithValue(DBNull.Value));
            else
                values.Add("NULL");
        }

        private void AddNonNullValue(object data, List<string> values, string destColumnName)
        {
            if (UseParameterQuery)
                values.Add(CreateParameterWithValue(data));
            else
            {
                string value = data.ToString().Replace("'", "''");
                string valueSql = $"'{value}'";
                values.Add(valueSql);
            }
        }

        private string CreateParameterWithValue(object parValue)
        {
            var par = new T();
            if (ConnectionType == ConnectionManagerType.Oracle && parValue is Enum) //Enums don't work obviously
                par.Value = (int)parValue;
            par.Value = parValue;
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
            QueryText.AppendLine($@"DELETE dt FROM {TN.QuotatedFullName} dt
INNER JOIN (");            
            
            if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine($" SELECT {string.Join(",", ColumnNames.Select(col => QB + col + QE))} FROM (");
            else
                QueryText.AppendLine("VALUES");            
        }

        private void AppendValueListSql(List<string> values, bool lastItem)
        {
            if (ConnectionType == ConnectionManagerType.Oracle)
            {
                QueryText.Append("SELECT ");
                for (int i = 0; i < values.Count; i++)
                {
                    QueryText.Append($"{values[i]} {QB}{ColumnNames[i]}{QE}");
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
            if (ConnectionType == ConnectionManagerType.Oracle)
                QueryText.AppendLine(")");
            QueryText.AppendLine($@") 
AS dv ( {string.Join(", ", ColumnNames.Select(col => QB + col + QE))} )
ON {string.Join(Environment.NewLine + " AND ", ColumnNames.Select(col => $"dt.{QB}{col}{QE} = dv.{QB}{col}{QE}"))}"); 
            
        }

        #endregion
    }
}
