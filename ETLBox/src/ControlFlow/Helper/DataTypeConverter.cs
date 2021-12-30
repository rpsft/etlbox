using ETLBox.Connection;
using System;
using System.Collections.Generic;
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
        public static int GetStringLengthFromCharString(string value) {
            if (string.IsNullOrEmpty(value)) return 0;
            string possibleResult = Regex.Replace(value, _REGEX, "${2}", RegexOptions.IgnoreCase);
            int result;
            int.TryParse(possibleResult, out result);
            return result;
        }

        internal static string GetNETObjectTypeString(string dbSpecificTypeName) {
            if (dbSpecificTypeName.IndexOf("(") > 0)
                dbSpecificTypeName = dbSpecificTypeName.Substring(0, dbSpecificTypeName.IndexOf("("));
            dbSpecificTypeName = dbSpecificTypeName.Trim().ToLower();
            switch (dbSpecificTypeName) {
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
                case "decfloat":
                    return "System.Decimal";
                case "real":
                case "float4":
                case "binary_float":
                    return "System.Single";
                case "float": //Float is 64bit inSql Server 
                case "float8":
                case "double":
                case "double precision":
                case "binary_double":
                    return "System.Double";
                case "date":
                case "datetime":
                case "smalldatetime":
                case "datetime2":
                case "timestamp":
                case "timestamptz":
                    return "System.DateTime";
                case "interval":
                case "time":
                    return "System.TimeSpan";
                case "timetz":
                case "datetimeoffset":
                    return "System.DateTimeOffset";
                case "uniqueidentifier":
                case "uuid":
                    return "System.Guid";
                case "binary":
                case "varbinary":
                case "bytea":
                case "blob":
                case "image":
                case "raw":
                case "graphic":
                case "vargraphic":
                case "bfile":
                    return "System.Byte[]";
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
        public static Type GetTypeObject(string dbSpecificTypeName) => Type.GetType(GetNETObjectTypeString(dbSpecificTypeName));

        /// <summary>
        /// Returns the ADO.NET System.Data.DbType object for a specific sql type.
        /// E.g. the method would return the System.Data.DbType.String for the sql type 'CHAR(10)'
        /// </summary>
        /// <param name="dbSpecificTypeName">The sql specific data type name</param>
        /// <returns>The corresponding ADO .NET database type</returns>
        public static DbType? GetDBType(string dbSpecificTypeName) {
            if (string.IsNullOrEmpty(dbSpecificTypeName)) return null;
            try {
                //return (DbType)Enum.Parse(typeof(DbType), GetNETObjectTypeString(dbSpecificTypeName).Replace("System.", ""), true);
                return GetDBType(GetTypeObject(dbSpecificTypeName));
            } catch {
                return DbType.String;
            }
        }

        public static DbType GetDBType(Type clrType) {
            var typeMap = new Dictionary<Type, DbType>();
            typeMap[typeof(byte)] = DbType.Byte;
            typeMap[typeof(sbyte)] = DbType.SByte;
            typeMap[typeof(short)] = DbType.Int16;
            typeMap[typeof(ushort)] = DbType.UInt16;
            typeMap[typeof(int)] = DbType.Int32;
            typeMap[typeof(uint)] = DbType.UInt32;
            typeMap[typeof(long)] = DbType.Int64;
            typeMap[typeof(ulong)] = DbType.UInt64;
            typeMap[typeof(float)] = DbType.Single;
            typeMap[typeof(double)] = DbType.Double;
            typeMap[typeof(decimal)] = DbType.Decimal;
            typeMap[typeof(bool)] = DbType.Boolean;
            typeMap[typeof(string)] = DbType.String;
            typeMap[typeof(char)] = DbType.StringFixedLength;
            typeMap[typeof(Guid)] = DbType.Guid;
            typeMap[typeof(DateTime)] = DbType.DateTime;
            typeMap[typeof(DateTimeOffset)] = DbType.DateTimeOffset;
            typeMap[typeof(byte[])] = DbType.Binary;
            typeMap[typeof(byte?)] = DbType.Byte;
            typeMap[typeof(sbyte?)] = DbType.SByte;
            typeMap[typeof(short?)] = DbType.Int16;
            typeMap[typeof(ushort?)] = DbType.UInt16;
            typeMap[typeof(int?)] = DbType.Int32;
            typeMap[typeof(uint?)] = DbType.UInt32;
            typeMap[typeof(long?)] = DbType.Int64;
            typeMap[typeof(ulong?)] = DbType.UInt64;
            typeMap[typeof(float?)] = DbType.Single;
            typeMap[typeof(double?)] = DbType.Double;
            typeMap[typeof(decimal?)] = DbType.Decimal;
            typeMap[typeof(bool?)] = DbType.Boolean;
            typeMap[typeof(char?)] = DbType.StringFixedLength;
            typeMap[typeof(Guid?)] = DbType.Guid;
            typeMap[typeof(DateTime?)] = DbType.DateTime;
            typeMap[typeof(DateTimeOffset?)] = DbType.DateTimeOffset;
            if (typeMap.ContainsKey(clrType))
                return typeMap[clrType];
            else
                return DbType.String;
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
        public static string TryGetDbSpecificType(string dataTypeName, ConnectionManagerType connectionType) {
            var typeName = dataTypeName.Trim().ToUpper();
            //Always normalize to some "standard" for Oracle!
            //https://docs.microsoft.com/en-us/sql/relational-databases/replication/non-sql/data-type-mapping-for-oracle-publishers?view=sql-server-ver15
            if (connectionType != ConnectionManagerType.Oracle) {
                if (typeName.StartsWith("NUMBER"))
                    return typeName.Replace("NUMBER", "NUMERIC");
                if (typeName.StartsWith("VARCHAR2"))
                    return typeName.Replace("VARCHAR2", "VARCHAR");
                else if (typeName.StartsWith("NVARCHAR2"))
                    return typeName.Replace("NVARCHAR2", "NVARCHAR");
            }

            //Now start with "normal" translation, other Database have many commons
            if (connectionType == ConnectionManagerType.SqlServer) {
                if (typeName.Replace(" ", "") == "TEXT")
                    return "VARCHAR(MAX)";
                return dataTypeName;
            } else if (connectionType == ConnectionManagerType.Access) {
                if (typeName == "INT")
                    return "INTEGER";
                else if (IsCharTypeDefinition(typeName)) {
                    if (typeName.StartsWith("N"))
                        typeName = typeName.Substring(1);
                    if (GetStringLengthFromCharString(typeName) > 255)
                        return "LONGTEXT";
                    return typeName;
                }
                return dataTypeName;
            } else if (connectionType == ConnectionManagerType.SQLite) {
                if (typeName == "INT" || typeName == "BIGINT")
                    return "INTEGER";
                return dataTypeName;
            } else if (connectionType == ConnectionManagerType.Postgres) {
                if (IsCharTypeDefinition(typeName)) {
                    if (typeName.StartsWith("N"))
                        return typeName.Substring(1);
                } else if (typeName == "DATETIME")
                    return "TIMESTAMP";
                else if (typeName.StartsWith("VARBINARY") || typeName.StartsWith("BINARY"))
                    return "BYTEA";
                return dataTypeName;
            } else if (connectionType == ConnectionManagerType.Db2) {
                if (typeName == "TEXT")
                    return "CLOB";
                return dataTypeName;
            } else if (connectionType == ConnectionManagerType.Oracle) {
                if (IsCharTypeDefinition(typeName)) {
                    if (typeName.Replace(" ", "").StartsWith("NVARCHAR("))
                        return typeName.Replace("NVARCHAR", "NVARCHAR2");
                    else if (typeName.Replace(" ", "").StartsWith("VARCHAR("))
                        return typeName.Replace("VARCHAR", "VARCHAR2");
                } else if (typeName.StartsWith("BINARY") && !typeName.StartsWith("BINARY_"))
                    return typeName.Replace("BINARY", "RAW");
                else if (typeName == "BIGINT")
                    return "INT";
                else if (typeName == "DATETIME")
                    return "DATE";
                else if (typeName == "FLOAT")
                    return "FLOAT(126)";
                else if (typeName == "TEXT")
                    return "NCLOB";
                return dataTypeName;
            } else {
                return dataTypeName;
            }
        }

        /// <summary>
        /// Converts a data type alias name (e.g. an alias name
        /// like "varchar(10)" ) to the original database type name ("character varying").
        /// </summary>
        /// <param name="dataTypeName">The database alias type name</param>
        /// <param name="connectionType">Which database (e.g. Postgres, MySql, ...)</param>
        /// <returns>The type name converted to an original database type name</returns>
        public static string TryConvertAliasName(string dataTypeName, ConnectionManagerType connectionType) {
            if (connectionType == ConnectionManagerType.Postgres) {
                //See https://www.postgresql.org/docs/9.5/datatype.html for aliases            
                dataTypeName = dataTypeName.ToLower().Trim();
                if (dataTypeName == "int8")
                    return "bigint";
                else if (dataTypeName == "serial8")
                    return "bigserial";
                else if (dataTypeName.StartsWith("varbit") || dataTypeName.StartsWith("bit varying"))
                    return "bit varying";
                else if (dataTypeName == "bool")
                    return "boolean";
                else if (dataTypeName.StartsWith("char") || dataTypeName.StartsWith("nchar"))
                    return "character";
                else if (dataTypeName.StartsWith("varchar") || dataTypeName.StartsWith("nvarchar"))
                    return "character varying";
                else if (dataTypeName == "float8")
                    return "double precision";
                else if (dataTypeName == "int" || dataTypeName == "int4")
                    return "integer";
                else if (dataTypeName.StartsWith("decimal") || dataTypeName.StartsWith("numeric"))
                    return "numeric";
                else if (dataTypeName == "float")
                    return "real";
                else if (dataTypeName == "int2")
                    return "smallint";
                else if (dataTypeName == "serial2")
                    return "smallserial";
                else if (dataTypeName == "serial4")
                    return "serial";
                else if (dataTypeName == "timestamptz")
                    return "timestamp with time zone";
                else if (dataTypeName.StartsWith("timestamp") && dataTypeName.EndsWith("with time zone"))
                    return "timestamp with time zone";
                else if (dataTypeName.StartsWith("timestamp"))
                    return "timestamp without time zone";
                else if (dataTypeName == "timetz")
                    return "time with time zone";
                else if (dataTypeName.StartsWith("time") && dataTypeName.EndsWith("with time zone"))
                    return "time with time zone";
                else if (dataTypeName.StartsWith("time"))
                    return "time without time zone";
                else if (dataTypeName.StartsWith("bit"))
                    return "bit";


                else
                    return dataTypeName;
            }

            return dataTypeName;
        }

        private static readonly Dictionary<DbType, Type> DbType2Type = new Dictionary<DbType, Type>
        {
            { DbType.Byte, typeof(byte) },
            { DbType.SByte, typeof(sbyte) },
            { DbType.Int16, typeof(short) },
            { DbType.UInt16, typeof(ushort) },
            { DbType.Int32, typeof(int) },
            { DbType.UInt32, typeof(uint) },
            { DbType.Int64, typeof(long) },
            { DbType.UInt64, typeof(ulong) },
            { DbType.Single, typeof(float) },
            { DbType.Double, typeof(double) },
            { DbType.Decimal, typeof(decimal) },
            { DbType.Boolean, typeof(bool) },
            { DbType.String, typeof(string) },
            { DbType.StringFixedLength, typeof(char) },
            { DbType.Guid, typeof(Guid) },
            { DbType.DateTime, typeof(DateTime) },
            { DbType.DateTimeOffset, typeof(DateTimeOffset) },
            { DbType.Binary, typeof(byte[]) }
        };

        /// <summary>
        /// Returns a .NET type for the provided DbType. 
        /// E.g. DbType.Binary will return byte[]
        /// </summary>
        /// <param name="dbType">The DbType</param>
        /// <returns>A .NET type</returns>
        public static Type GetClrType(DbType dbType) {
            Type type;
            if (DbType2Type.TryGetValue(dbType, out type)) {
                return type;
            }
            throw new ArgumentOutOfRangeException("dbType", dbType, "Cannot map the DbType to Type");
        }


        private static readonly Dictionary<Type, string> Type2SqlServerType = new Dictionary<Type, string> {
            { typeof(long),"BIGINT" },
            { typeof(int),"INT" },
            { typeof(short),"SMALLINT" },
            { typeof(byte),"TINYINT" },
            { typeof(byte[]),"VARBINARY(8000)" },
            { typeof(bool),"BIT" },
            { typeof(char),"CHAR" },
            { typeof(DateTime),"DATETIME2" },
            { typeof(DateTimeOffset),"DATETIMEOFFSET" },
            { typeof(decimal),"DECIMAL" },
            { typeof(double),"FLOAT" },
            { typeof(string),"NVARCHAR(4000)" },
            { typeof(Guid),"UNIQUEIDENTIFIER" },
            { typeof(TimeSpan),"TIME" },
        };

        /// <summary>
        /// Returns a database specific type for the provided .NET datat type, depending on the connection
        /// manager. E.g. passing the .NET data type long for SqlServer will return the string BIGINT 
        /// </summary>
        /// <param name="clrType">The .NET data type</param>
        /// <param name="connectionType">Database connection type, e.g. SqlServer</param>
        /// <returns>A database specific type string</returns>
        public static string GetDatabaseType(Type clrType, ConnectionManagerType connectionType) {
            if (connectionType == ConnectionManagerType.SqlServer) {
                if (Type2SqlServerType.ContainsKey(clrType))
                    return Type2SqlServerType[clrType];
                else
                    throw new ArgumentOutOfRangeException("clrType", clrType, "Cannot map the ClrType to database specific Type");
            } else {
                throw new ArgumentException("This connection type is not supported yet!", nameof(connectionType));
            }
        }
    }
}
