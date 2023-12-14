﻿using System.Text.RegularExpressions;

namespace ALE.ETLBox.src.Definitions.ConnectionManager
{
    [PublicAPI]
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

        private const string Regex = @"(.*?)char\((\d*)\)(.*?)";

        public static bool IsCharTypeDefinition(string value) =>
            new Regex(Regex, RegexOptions.IgnoreCase).IsMatch(value);

        public static int GetStringLengthFromCharString(string value)
        {
            var possibleResult = System.Text.RegularExpressions.Regex.Replace(
                value,
                Regex,
                "${2}",
                RegexOptions.IgnoreCase
            );
            return int.TryParse(possibleResult, out var result) ? result : DefaultStringLength;
        }

        public static string GetNETObjectTypeString(string dbSpecificTypeName)
        {
            if (dbSpecificTypeName.IndexOf("(", StringComparison.Ordinal) >= 1)
                dbSpecificTypeName = dbSpecificTypeName.Substring(
                    0,
                    dbSpecificTypeName.IndexOf("(", StringComparison.Ordinal)
                );
            dbSpecificTypeName = dbSpecificTypeName.Trim().ToLower();
            return dbSpecificTypeName switch
            {
                "bit" => "System.Boolean",
                "boolean" => "System.Boolean",
                "tinyint" => "System.UInt16",
                "smallint" => "System.Int16",
                "int2" => "System.Int16",
                "int" => "System.Int32",
                "int4" => "System.Int32",
                "int8" => "System.Int32",
                "integer" => "System.Int32",
                "bigint" => "System.Int64",
                "decimal" => "System.Decimal",
                "number" => "System.Decimal",
                "money" => "System.Decimal",
                "smallmoney" => "System.Decimal",
                "numeric" => "System.Decimal",
                "real" => "System.Double",
                "float" => "System.Double",
                "float4" => "System.Double",
                "float8" => "System.Double",
                "double" => "System.Double",
                "double precision" => "System.Double",
                "date" => "System.DateTime",
                "datetime" => "System.DateTime",
                "smalldatetime" => "System.DateTime",
                "datetime2" => "System.DateTime",
                "time" => "System.DateTime",
                "timetz" => "System.DateTime",
                "timestamp" => "System.DateTime",
                "timestamptz" => "System.DateTime",
                "uniqueidentifier" => "System.Guid",
                "uuid" => "System.Guid",
                _ => "System.String"
            };
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
            switch (connectionType)
            {
                case ConnectionManagerType.SqlServer when typeName.Replace(" ", "") == "TEXT":
                    return "VARCHAR(MAX)";
                case ConnectionManagerType.Access when typeName == "INT":
                    return "INTEGER";
                case ConnectionManagerType.Access when IsCharTypeDefinition(typeName):
                    {
                        if (typeName.StartsWith("N"))
                            typeName = typeName.Substring(1);
                        return GetStringLengthFromCharString(typeName) > 255 ? "LONGTEXT" : typeName;
                    }
                case ConnectionManagerType.Access:
                    return dbSpecificTypeName;
                case ConnectionManagerType.SQLite when typeName is "INT" or "BIGINT":
                    return "INTEGER";
                case ConnectionManagerType.SQLite:
                    return dbSpecificTypeName;
                case ConnectionManagerType.Postgres:
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
                case ConnectionManagerType.Unknown:
                case ConnectionManagerType.Adomd:
                case ConnectionManagerType.MySql:
                default:
                    return dbSpecificTypeName;
            }
        }

        public static DateTimeKind? GetNETDateTimeKind(string dbSpecificTypeName)
        {
            return dbSpecificTypeName switch
            {
                "date" => DateTimeKind.Unspecified,
                "datetime" => DateTimeKind.Unspecified,
                "smalldatetime" => DateTimeKind.Unspecified,
                "datetime2" => DateTimeKind.Unspecified,
                "time" => DateTimeKind.Unspecified,
                "timestamp" => DateTimeKind.Unspecified,
                "timetz" => DateTimeKind.Utc,
                "timestamptz" => DateTimeKind.Utc,
                _ => null
            };
        }
    }
}
