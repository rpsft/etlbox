using System.Data;
using System.Text.RegularExpressions;

namespace ALE.ETLBox.ConnectionManager
{
    public static class DataTypeConverter
    {
        public const int DefaultTinyIntegerLength = 5;
        public const int DefaultSmallIntegerLength = 7;
        public const int DefaultIntegerLength = 11;
        public const int DefaultBigIntegerLength = 21;
        public const int DefaultDateTime2Length = 41;
        public const int DefaultDateTimeLength = 27;
        public const int DefaultDecimalLength = 41;
        public const int DefaultStringLength = 255;

        public const string _REGEX = @"(.*?)char\((\d*)\)(.*?)";

        public static int GetTypeLength(string dataTypeString)
        {
            switch (dataTypeString)
            {
                case "tinyint":
                    return DefaultTinyIntegerLength;
                case "smallint":
                    return DefaultSmallIntegerLength;
                case "int":
                    return DefaultIntegerLength;
                case "bigint":
                    return DefaultBigIntegerLength;
                case "decimal":
                    return DefaultDecimalLength;
                case "datetime":
                    return DefaultDateTimeLength;
                case "datetime2":
                    return DefaultDateTime2Length;
                default:
                    if (IsCharTypeDefinition(dataTypeString))
                        return GetStringLengthFromCharString(dataTypeString);
                    throw new Exception("Unknown data type");
            }
        }

        public static bool IsCharTypeDefinition(string value) =>
            new Regex(_REGEX, RegexOptions.IgnoreCase).IsMatch(value);

        public static int GetStringLengthFromCharString(string value)
        {
            string possibleResult = Regex.Replace(value, _REGEX, "${2}", RegexOptions.IgnoreCase);
            int result = 0;
            if (int.TryParse(possibleResult, out result))
            {
                return result;
            }

            return DefaultStringLength;
        }

        public static string GetNETObjectTypeString(string dbSpecificTypeName)
        {
            if (dbSpecificTypeName.IndexOf("(") > 0)
                dbSpecificTypeName = dbSpecificTypeName.Substring(
                    0,
                    dbSpecificTypeName.IndexOf("(")
                );
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
                return (DbType)
                    Enum.Parse(
                        typeof(DbType),
                        GetNETObjectTypeString(dbSpecificTypeName).Replace("System.", ""),
                        true
                    );
            }
            catch
            {
                return DbType.String;
            }
        }

        public static string TryGetDBSpecificType(
            string dbSpecificTypeName,
            ConnectionManagerType connectionType
        )
        {
            var typeName = dbSpecificTypeName.Trim().ToUpper();
            if (connectionType == ConnectionManagerType.SqlServer)
            {
                if (typeName.Replace(" ", "") == "TEXT")
                    return "VARCHAR(MAX)";
            }
            if (connectionType == ConnectionManagerType.Access)
            {
                if (typeName == "INT")
                    return "INTEGER";
                if (IsCharTypeDefinition(typeName))
                {
                    if (typeName.StartsWith("N"))
                        typeName = typeName.Substring(1);
                    if (GetStringLengthFromCharString(typeName) > 255)
                        return "LONGTEXT";
                    return typeName;
                }
                return dbSpecificTypeName;
            }

            if (connectionType == ConnectionManagerType.SQLite)
            {
                if (typeName == "INT" || typeName == "BIGINT")
                    return "INTEGER";
                return dbSpecificTypeName;
            }

            if (connectionType == ConnectionManagerType.Postgres)
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

            return dbSpecificTypeName;
        }

        public static DateTimeKind? GetNETDateTimeKind(string dbSpecificTypeName)
        {
            switch (dbSpecificTypeName)
            {
                case "date":
                case "datetime":
                case "smalldatetime":
                case "datetime2":
                case "time":
                case "timestamp":
                    return DateTimeKind.Unspecified;
                case "timetz":
                case "timestamptz":
                    return DateTimeKind.Utc;
                default:
                    return null;
            }
        }
    }
}
