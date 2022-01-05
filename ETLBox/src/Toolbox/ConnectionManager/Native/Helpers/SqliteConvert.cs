using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Data.Sqlite;

namespace ALE.ETLBox.ConnectionManager.Helpers;

/// <summary>
/// This base class provides datatype conversion services for the Sqlite provider.
/// </summary>
public abstract class SqliteConvert
{
    /// <summary>
    /// For a given intrinsic type, return a DbType
    /// </summary>
    /// <param name="typ">The native type to convert</param>
    /// <returns>The corresponding (closest match) DbType</returns>
    private static DbType TypeToDbType(Type typ)
    {
        var tc = Type.GetTypeCode(typ);
        if (tc == TypeCode.Object)
        {
            if (typ == typeof(byte[])) return DbType.Binary;
            if (typ == typeof(Guid)) return DbType.Guid;
            return DbType.String;
        }

        return DbTypeMappings[(int)tc];
    }

    private static readonly DbType[] DbTypeMappings =
    {
        DbType.Object, // Empty (0)
        DbType.Binary, // Object (1)
        DbType.Object, // DBNull (2)
        DbType.Boolean, // Boolean (3)
        DbType.SByte, // Char (4)
        DbType.SByte, // SByte (5)
        DbType.Byte, // Byte (6)
        DbType.Int16, // Int16 (7)
        DbType.UInt16, // UInt16 (8)
        DbType.Int32, // Int32 (9)
        DbType.UInt32, // UInt32 (10)
        DbType.Int64, // Int64 (11)
        DbType.UInt64, // UInt64 (12)
        DbType.Single, // Single (13)
        DbType.Double, // Double (14)
        DbType.Decimal, // Decimal (15)
        DbType.DateTime, // DateTime (16)
        DbType.Object, // ?? (17)
        DbType.String // String (18)
    };

    public static DbType TypeToDbType(object _objValue)
    {
        if (_objValue != null && _objValue != DBNull.Value)
        {
            return TypeToDbType(_objValue.GetType());
        }

        return DbType.String; // Unassigned default value is String
    }

    public static SqliteType TypeToAffinity(object _objValue)
    {
        if (_objValue != null && _objValue != DBNull.Value)
        {
            return TypeToAffinity(_objValue.GetType());
        }

        return SqliteType.Text; // Unassigned default value is String
    }

    /// <summary>
    /// For a given type, return the closest-match SQLite TypeAffinity, which only understands a very limited subset of types.
    /// </summary>
    /// <param name="typ">The type to evaluate</param>
    /// <param name="flags">The flags associated with the connection.</param>
    /// <returns>The SQLite type affinity for that type.</returns>
    public static SqliteType TypeToAffinity(Type typ)
    {
        var tc = Type.GetTypeCode(typ);
        if (tc == TypeCode.Object)
        {
            if (typ == typeof(byte[]) || typ == typeof(Guid))
                return SqliteType.Blob;
            else
                return SqliteType.Text;
        }

        return TypeCodeAffinities[(int)tc];
    }

    private static readonly SqliteType[] TypeCodeAffinities =
    {
        SqliteType.Text, // Empty (0)
        SqliteType.Blob, // Object (1)
        SqliteType.Text, // DBNull (2)
        SqliteType.Integer, // Boolean (3)
        SqliteType.Integer, // Char (4)
        SqliteType.Integer, // SByte (5)
        SqliteType.Integer, // Byte (6)
        SqliteType.Integer, // Int16 (7)
        SqliteType.Integer, // UInt16 (8)
        SqliteType.Integer, // Int32 (9)
        SqliteType.Integer, // UInt32 (10)
        SqliteType.Integer, // Int64 (11)
        SqliteType.Integer, // UInt64 (12)
        SqliteType.Real, // Single (13)
        SqliteType.Real, // Double (14)
        SqliteType.Real, // Decimal (15)
        SqliteType.Text, // DateTime (16)
        SqliteType.Text, // ?? (17)
        SqliteType.Text // String (18)
    };
}