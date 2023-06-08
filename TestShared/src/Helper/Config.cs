using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using Microsoft.Extensions.Configuration;

namespace TestShared.Helper;

public static class Config
{
    private static IConfigurationRoot _defaultConfigFile;

    public static ConnectionDetails<
        SqlConnectionString,
        SqlConnectionManager
    > SqlConnection { get; } = new("SqlConnectionString");

    public static ConnectionDetails<
        SqlConnectionString,
        AdomdConnectionManager
    > SSASConnection { get; } = new("SSASConnectionString");

    public static ConnectionDetails<
        SQLiteConnectionString,
        SQLiteConnectionManager
    > SQLiteConnection { get; set; } = new("SQLiteConnectionString");

    public static ConnectionDetails<
        MySqlConnectionString,
        MySqlConnectionManager
    > MySqlConnection { get; } = new("MySqlConnectionString");

    public static ConnectionDetails<
        PostgresConnectionString,
        PostgresConnectionManager
    > PostgresConnection { get; } = new("PostgresConnectionString");

    public static ConnectionDetails<
        OdbcConnectionString,
        AccessOdbcConnectionManager
    > AccessOdbcConnection { get; } = new("AccessOdbcConnectionString");

    public static ConnectionDetails<
        OdbcConnectionString,
        SqlOdbcConnectionManager
    > SqlOdbcConnection { get; } = new("SqlOdbcConnectionString");

    public static ConnectionDetails<
        SqlConnectionString,
        SqlConnectionManager
    > AzureSqlConnection { get; } = new("AzureSqlConnectionString");

    private static IConfigurationRoot DefaultConfigFile
    {
        get
        {
            if (_defaultConfigFile == null)
            {
                var environmentVariable = Environment.GetEnvironmentVariable("ETLBoxConfig");
                var path = string.IsNullOrWhiteSpace(environmentVariable)
                    ? "default.config.json"
                    : environmentVariable;
                Load(path);
            }

            return _defaultConfigFile;
        }
        set => _defaultConfigFile = value;
    }

    public static IEnumerable<object[]> AllSqlConnections(string section)
    {
        return new[]
        {
            new object[] { SqlConnection.ConnectionManager(section) },
            new object[] { PostgresConnection.ConnectionManager(section) },
            new object[] { MySqlConnection.ConnectionManager(section) },
            new object[] { SQLiteConnection.ConnectionManager(section) }
        };
    }

    public static IEnumerable<object[]> AllConnectionsWithoutSQLite(string section)
    {
        return new[]
        {
            new object[] { SqlConnection.ConnectionManager(section) },
            new object[] { PostgresConnection.ConnectionManager(section) },
            new object[] { MySqlConnection.ConnectionManager(section) }
        };
    }

    public static IEnumerable<object[]> AllSqlConnectionsWithValue(string section, string value)
    {
        return new[]
        {
            new object[] { SqlConnection.ConnectionManager(section), value },
            new object[] { PostgresConnection.ConnectionManager(section), value },
            new object[] { MySqlConnection.ConnectionManager(section), value },
            new object[] { SQLiteConnection.ConnectionManager(section), value }
        };
    }

    public static IEnumerable<object[]> AllSqlConnectionsWithValue(string section, int value)
    {
        return new[]
        {
            new object[] { SqlConnection.ConnectionManager(section), value },
            new object[] { PostgresConnection.ConnectionManager(section), value },
            new object[] { MySqlConnection.ConnectionManager(section), value },
            new object[] { SQLiteConnection.ConnectionManager(section), value }
        };
    }

    public static IEnumerable<object[]> AllOdbcConnections(string section)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? new[]
            {
                new object[] { SqlOdbcConnection.ConnectionManager(section) },
                new object[] { AccessOdbcConnection.ConnectionManager(section) }
            }
            : Array.Empty<object[]>();
    }

    public static IEnumerable<object[]> AccessConnection(string section)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? new[] { new object[] { AccessOdbcConnection.ConnectionManager(section) } }
            : Array.Empty<object[]>();
    }

    public static void Load(string configFile)
    {
        DefaultConfigFile = new ConfigurationBuilder().AddJsonFile(configFile).Build();
    }

    public static IEnumerable<CultureInfo> AllLocalCultures()
    {
        return new[] { CultureInfo.GetCultureInfo("ru-RU"), CultureInfo.GetCultureInfo("en-US") };
    }

    public class ConnectionDetails<TConnectionString, TConnectionManager>
        where TConnectionString : IDbConnectionString, new()
        where TConnectionManager : IConnectionManager, new()
    {
        public ConnectionDetails(string connectionStringName)
        {
            ConnectionStringName = connectionStringName;
        }

        public string ConnectionStringName { get; set; }

        public string RawConnectionString(string section)
        {
            return DefaultConfigFile.GetSection(section)[ConnectionStringName];
        }

        public TConnectionString ConnectionString(string section)
        {
            return new TConnectionString { Value = RawConnectionString(section) };
        }

        public TConnectionManager ConnectionManager(string section)
        {
            return new TConnectionManager { ConnectionString = ConnectionString(section) };
        }
    }
}
