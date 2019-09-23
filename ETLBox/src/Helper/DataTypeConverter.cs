using ALE.ETLBox.ConnectionManager;
using System;
using System.Data;
using System.Text.RegularExpressions;

namespace ALE.ETLBox.Helper
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
                case "tinyint": return DefaultTinyIntegerLength;
                case "smallint": return DefaultSmallIntegerLength;
                case "int": return DefaultIntegerLength;
                case "bigint": return DefaultBigIntegerLength;
                case "decimal": return DefaultDecimalLength;
                case "datetime": return DefaultDateTimeLength;
                case "datetime2": return DefaultDateTime2Length;
                default:
                    if (IsCharTypeDefinition(dataTypeString))
                        return GetStringLengthFromCharString(dataTypeString);
                    else
                        throw new Exception("Unknown data type");
            }
        }

        public static bool IsCharTypeDefinition(string value) => new Regex(_REGEX, RegexOptions.IgnoreCase).IsMatch(value);

        public static int GetStringLengthFromCharString(string value)
        {
            string possibleResult = Regex.Replace(value, _REGEX, "${2}", RegexOptions.IgnoreCase);
            int result = 0;
            if (int.TryParse(possibleResult, out result))
            {
                return result;
            }
            else
            {
                return DefaultStringLength;
            }
        }

        public static string GetNETObjectTypeString(string dbSpecificTypeName)
        {
            if (dbSpecificTypeName.IndexOf("(") > 0)
                dbSpecificTypeName = dbSpecificTypeName.Substring(0, dbSpecificTypeName.IndexOf("("));
            dbSpecificTypeName = dbSpecificTypeName.Trim().ToLower();
            switch (dbSpecificTypeName)
            {
                case "bit": return "System.Boolean";
                case "tinyint": return "System.UInt16";
                case "smallint": return "System.Int16";
                case "int": return "System.Int32";
                case "bigint": return "System.Int64";
                case "decimal": return "System.Decimal";
                case "number": return "System.Decimal";
                case "datetime": return "System.DateTime";
                case "datetime2": return "System.DateTime";
                case "uniqueidentifier": return "System.Guid";
                default: return "System.String";
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

        public static string TryGetDBSpecificType(string dbSpecificTypeName, ConnectionManagerType connectionType)
        {
            var typeName = dbSpecificTypeName.Trim().ToUpper();
            if (connectionType == ConnectionManagerType.Access)
            {
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
            else if (connectionType == ConnectionManagerType.SQLite)
            {
                if (typeName == "INT")
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
                return dbSpecificTypeName;
            }
            else
            {
                return dbSpecificTypeName;
            }
        }
    }
}
