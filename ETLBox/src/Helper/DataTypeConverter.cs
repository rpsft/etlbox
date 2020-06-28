using ETLBox.Connection;
using System;
using System.ComponentModel;
using System.Data;
using System.Text.RegularExpressions;

namespace ETLBox.Helper
{
    public class DataTypeConverter : IDataTypeConverter
    {
        public const string _REGEX = @"(.*?)char\((\d*)\)(.*?)";

        public static bool IsCharTypeDefinition(string value) => new Regex(_REGEX, RegexOptions.IgnoreCase).IsMatch(value);

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

        public static Type GetTypeObject(string dbSpecificTypeName)
        {
            return Type.GetType(GetNETObjectTypeString(dbSpecificTypeName));
        }

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

        public string TryConvertDbDataType(string dbSpecificTypeName, ConnectionManagerType connectionType)
            => DataTypeConverter.TryGetDbSpecificType(dbSpecificTypeName, connectionType);

        public static string TryGetDbSpecificType(string dbSpecificTypeName, ConnectionManagerType connectionType)
        {
            var typeName = dbSpecificTypeName.Trim().ToUpper();
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
                return dbSpecificTypeName;
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
                return dbSpecificTypeName;
            }
            else if (connectionType == ConnectionManagerType.SQLite)
            {
                if (typeName == "INT" || typeName == "BIGINT")
                    return "INTEGER";
                return dbSpecificTypeName;
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
                return dbSpecificTypeName;
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
                return dbSpecificTypeName;
            }
            else
            {
                return dbSpecificTypeName;
            }
        }
    }
}
