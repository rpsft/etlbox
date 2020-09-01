using ETLBox.Connection;
using System;
using System.ComponentModel;
using System.Data;
using System.Text.RegularExpressions;

namespace ETLBox.Helper
{
    /// <summary>
    /// This class provides static methods and an implementation of IDataTypeConverter that converts
    /// various sql data types into the right database specific database or into a .NET data type.
    /// </summary>
    public class DataTypeConverter : IDataTypeConverter
    {

        const string _REGEX = @"(.*?)char\((\d*)\)(.*?)";

        static bool IsCharTypeDefinition(string value) => new Regex(_REGEX, RegexOptions.IgnoreCase).IsMatch(value);

        /// <summary>
        /// Returns the string length that a sql char datatype has in it's definition.
        /// E.g. VARCHAR(40) would return 40, NVARCHAR2 ( 2 ) returns 2
        /// </summary>
        /// <param name="value">A sql character data type</param>
        /// <returns>The string length defined in the data type - 0 if nothing could be found</returns>
        public static int GetStringLengthFromCharString(string value)
        {
            string possibleResult = Regex.Replace(value, _REGEX, "${2}", RegexOptions.IgnoreCase);
            int result = 0;
            int.TryParse(possibleResult, out result);
            return result;
        }

        private static string GetNETObjectTypeString(string dbSpecificTypeName)
        {
            if (dbSpecificTypeName.IndexOf("(") > 0)
                dbSpecificTypeName = dbSpecificTypeName.Substring(0, dbSpecificTypeName.IndexOf("("));
            dbSpecificTypeName = dbSpecificTypeName.Trim().ToLower();
            switch (dbSpecificTypeName)
            {
                case "bit":
                case "boolean":
                    return "System.Boolean";
                case "tinyint":
                    return "System.UInt16";
                case "smallint":
                case "int2":
                    return "System.Int16";
                case "int":
                case "int4":
                case "int8":
                case "integer":
                    return "System.Int32";
                case "bigint":
                    return "System.Int64";
                case "decimal":
                case "number":
                case "money":
                case "smallmoney":
                case "numeric":
                    return "System.Decimal";
                case "real":
                case "float":
                case "float4":
                case "float8":
                case "double":
                case "double precision":
                    return "System.Double";
                case "date":
                case "datetime":
                case "smalldatetime":
                case "datetime2":
                case "time":
                case "timetz":
                case "timestamp":
                case "timestamptz":
                    return "System.DateTime";
                case "uniqueidentifier":
                case "uuid":
                    return "System.Guid";
                default:
                    return "System.String";
            }
        }

        /// <summary>
        /// Returns the .NET type object for a specific sql type.
        /// E.g. the method would return the .NET type string for the sql type 'CHAR(10)'
        /// </summary>
        /// <param name="dbSpecificTypeName">The sql specific data type name</param>
        /// <returns>The corresponding .NET data type</returns>
        public static Type GetTypeObject(string dbSpecificTypeName) =>Type.GetType(GetNETObjectTypeString(dbSpecificTypeName));

        /// <summary>
        /// Returns the ADO.NET System.Data.DbType object for a specific sql type.
        /// E.g. the method would return the System.Data.DbType.String for the sql type 'CHAR(10)'
        /// </summary>
        /// <param name="dbSpecificTypeName">The sql specific data type name</param>
        /// <returns>The corresponding ADO .NET database type</returns>
        public static DbType GetDBType(string dbSpecificTypeName)
        {
            try
            {
                return (DbType)Enum.Parse(typeof(DbType), GetNETObjectTypeString(dbSpecificTypeName).Replace("System.", ""), true);
            }
            catch
            {
                return DbType.String;
            }
        }


        /// <inheritdoc/>
        public string TryConvertDbDataType(string dataTypeName, ConnectionManagerType connectionType)
            => DataTypeConverter.TryGetDbSpecificType(dataTypeName, connectionType);

        /// <summary>
        /// Tries to convert the data type into a database specific type.
        /// E.g. the data type 'INT' would be converted to 'INTEGER' for SQLite connections.
        /// </summary>
        /// <param name="dataTypeName">A data type name</param>
        /// <param name="connectionType">The database connection type</param>
        /// <returns>The converted database specific type name</returns>
        public static string TryGetDbSpecificType(string dataTypeName, ConnectionManagerType connectionType)
        {
            var typeName = dataTypeName.Trim().ToUpper();
            //Always normalize to some "standard" for Oracle!
            //https://docs.microsoft.com/en-us/sql/relational-databases/replication/non-sql/data-type-mapping-for-oracle-publishers?view=sql-server-ver15
            if (connectionType != ConnectionManagerType.Oracle)
            {
                if (typeName.StartsWith("NUMBER"))
                    return typeName.Replace("NUMBER", "NUMERIC");
                if (typeName.StartsWith("VARCHAR2"))
                    return typeName.Replace("VARCHAR2", "VARCHAR");
                else if (typeName.StartsWith("NVARCHAR2"))
                    return typeName.Replace("NVARCHAR2", "NVARCHAR");
            }

            //Now start with "normal" translation, other Database have many commons
            if (connectionType == ConnectionManagerType.SqlServer)
            {
                if (typeName.Replace(" ", "") == "TEXT")
                    return "VARCHAR(MAX)";
                return dataTypeName;
            }
            else if (connectionType == ConnectionManagerType.Access)
            {
                if (typeName == "INT")
                    return "INTEGER";
                else if (IsCharTypeDefinition(typeName))
                {
                    if (typeName.StartsWith("N"))
                        typeName = typeName.Substring(1);
                    if (GetStringLengthFromCharString(typeName) > 255)
                        return "LONGTEXT";
                    return typeName;
                }
                return dataTypeName;
            }
            else if (connectionType == ConnectionManagerType.SQLite)
            {
                if (typeName == "INT" || typeName == "BIGINT")
                    return "INTEGER";
                return dataTypeName;
            }
            else if (connectionType == ConnectionManagerType.Postgres)
            {
                if (IsCharTypeDefinition(typeName))
                {
                    if (typeName.StartsWith("N"))
                        return typeName.Substring(1);
                }
                else if (typeName == "DATETIME")
                    return "TIMESTAMP";
                return dataTypeName;
            }
            else if (connectionType == ConnectionManagerType.Oracle)
            {
                if (IsCharTypeDefinition(typeName))
                {
                    if (typeName.Replace(" ","").StartsWith("NVARCHAR("))
                        return typeName.Replace("NVARCHAR","NVARCHAR2");
                    else if (typeName.Replace(" ", "").StartsWith("VARCHAR("))
                        return typeName.Replace("VARCHAR", "VARCHAR2");
                }
                else if (typeName == "BIGINT")
                    return "INT";
                else if (typeName == "DATETIME")
                    return "DATE";
                else if (typeName == "FLOAT")
                    return "FLOAT(126)";
                else if (typeName == "TEXT")
                    return "NCLOB";
                return dataTypeName;
            }
            else
            {
                return dataTypeName;
            }
        }
    }
}
