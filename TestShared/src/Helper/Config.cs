using ETLBox.Connection;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace ETLBoxTests.Helper
{

    public static class Config
    {
        public class ConnectionDetails<TConnectionString, TConnectionManager>
        where TConnectionString : IDbConnectionString, new()
        where TConnectionManager : IConnectionManager, new()
        {
            public string ConnectionStringName { get; set; }
            public ConnectionDetails(string connectionStringName)
            {
                this.ConnectionStringName = connectionStringName;
            }
            public string RawConnectionString(string section)
               => Config.DefaultConfigFile.GetSection(section)[ConnectionStringName];
            public TConnectionString ConnectionString(string section)
                => new TConnectionString() { Value = RawConnectionString(section) };
            public TConnectionManager ConnectionManager(string section)
                => new TConnectionManager() { ConnectionString = ConnectionString(section) };

        }

        public static ConnectionDetails<SqlConnectionString, SqlConnectionManager> SqlConnection
        { get; set; } = new ConnectionDetails<SqlConnectionString, SqlConnectionManager>("SqlConnectionString");

        public static ConnectionDetails<SqlConnectionString, AdomdConnectionManager> SSASConnection
        { get; set; } = new ConnectionDetails<SqlConnectionString, AdomdConnectionManager>("SSASConnectionString");

        public static ConnectionDetails<SQLiteConnectionString, SQLiteConnectionManager> SQLiteConnection
        { get; set; } = new ConnectionDetails<SQLiteConnectionString, SQLiteConnectionManager>("SQLiteConnectionString");

        public static ConnectionDetails<MySqlConnectionString, MySqlConnectionManager> MySqlConnection
        { get; set; } = new ConnectionDetails<MySqlConnectionString, MySqlConnectionManager>("MySqlConnectionString");

        public static ConnectionDetails<MariaDbConnectionString, MariaDbConnectionManager> MariaDbConnection
        { get; set; } = new ConnectionDetails<MariaDbConnectionString, MariaDbConnectionManager>("MariaDbConnectionString");

        public static ConnectionDetails<PostgresConnectionString, PostgresConnectionManager> PostgresConnection
        { get; set; } = new ConnectionDetails<PostgresConnectionString, PostgresConnectionManager>("PostgresConnectionString");

        public static ConnectionDetails<OracleConnectionString, OracleConnectionManager> OracleConnection
        { get; set; } = new ConnectionDetails<OracleConnectionString, OracleConnectionManager>("OracleConnectionString");

        public static ConnectionDetails<OdbcConnectionString, AccessOdbcConnectionManager> AccessOdbcConnection
        { get; set; } = new ConnectionDetails<OdbcConnectionString, AccessOdbcConnectionManager>("AccessOdbcConnectionString");

        public static ConnectionDetails<OdbcConnectionString, SqlOdbcConnectionManager> SqlOdbcConnection
        { get; set; } = new ConnectionDetails<OdbcConnectionString, SqlOdbcConnectionManager>("SqlOdbcConnectionString");

        public static ConnectionDetails<OdbcConnectionString, MySqlOdbcConnectionManager> MySqlOdbcConnection
        { get; set; } = new ConnectionDetails<OdbcConnectionString, MySqlOdbcConnectionManager>("MySqlOdbcConnectionString");

        public static ConnectionDetails<OdbcConnectionString, PostgresOdbcConnectionManager> PostgresOdbcConnection
        { get; set; } = new ConnectionDetails<OdbcConnectionString, PostgresOdbcConnectionManager>("PostgresOdbcConnectionString");

        public static ConnectionDetails<OdbcConnectionString, OracleOdbcConnectionManager> OracleOdbcConnection
        { get; set; } = new ConnectionDetails<OdbcConnectionString, OracleOdbcConnectionManager>("OracleOdbcConnectionString");

        public static ConnectionDetails<OleDbConnectionString, SqlOleDbConnectionManager> SqlOleDbConnection
        { get; set; } = new ConnectionDetails<OleDbConnectionString, SqlOleDbConnectionManager>("SqlOleDbConnectionString");

        public static ConnectionDetails<SqlConnectionString, SqlConnectionManager> AzureSqlConnection
        { get; set; } = new ConnectionDetails<SqlConnectionString, SqlConnectionManager>("AzureSqlConnectionString");


        public static IEnumerable<object[]> AllSqlConnections(string section) => new[] {
                    new object[] { (IConnectionManager)SqlConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)MariaDbConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)OracleConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)PostgresConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)MySqlConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)SQLiteConnection.ConnectionManager(section) },
        };

        public static IEnumerable<object[]> AllConnectionsWithoutSQLite(string section) => new[] {
                    new object[] { (IConnectionManager)MariaDbConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)OracleConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)SqlConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)PostgresConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)MySqlConnection.ConnectionManager(section) },
        };

        public static IEnumerable<object[]> AllSqlConnectionsWithValue(string section, string value) => new[] {
                    new object[] { (IConnectionManager)MariaDbConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)OracleConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)SqlConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)PostgresConnection.ConnectionManager(section), value },
                    new object[] { (IConnectionManager)MySqlConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)SQLiteConnection.ConnectionManager(section) , value},
        };

        public static IEnumerable<object[]> AllSqlConnectionsWithValue(string section, int value) => new[] {
                    new object[] { (IConnectionManager)MariaDbConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)OracleConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)SqlConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)PostgresConnection.ConnectionManager(section), value },
                    new object[] { (IConnectionManager)MySqlConnection.ConnectionManager(section) , value},
                    new object[] { (IConnectionManager)SQLiteConnection.ConnectionManager(section) , value}
        };

        public static IEnumerable<object[]> AllOdbcConnectionsExceptAccess(string section) => new[] {
                    new object[] { (IConnectionManager)OracleOdbcConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)SqlOdbcConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)MySqlOdbcConnection.ConnectionManager(section) },
                    new object[] { (IConnectionManager)PostgresOdbcConnection.ConnectionManager(section) }
        };

        public static IEnumerable<object[]> AllOleDbConnections(string section) => new[] {
                    new object[] { (IConnectionManager)SqlOleDbConnection.ConnectionManager(section) }
        };

        public static IEnumerable<object[]> AccessConnection(string section) => new[] {
                    new object[] { (IConnectionManager)AccessOdbcConnection.ConnectionManager(section) }
        };

        static IConfigurationRoot _defaultConfigFile;
        public static IConfigurationRoot DefaultConfigFile
        {
            get
            {
                if (_defaultConfigFile == null)
                {
                    var envvar = Environment.GetEnvironmentVariable("ETLBoxConfig");
                    var path = string.IsNullOrWhiteSpace(envvar) ? $"default.config.json" : envvar;
                    Load(path);
                }
                return _defaultConfigFile;
            }
            set
            {
                _defaultConfigFile = value;
            }
        }

        public static void Load(string configFile)
        {
            DefaultConfigFile = new ConfigurationBuilder()
                    .AddJsonFile(configFile)
                    .Build();
        }

    }
}
