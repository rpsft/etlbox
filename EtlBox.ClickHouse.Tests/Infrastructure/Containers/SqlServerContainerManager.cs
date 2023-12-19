// Copyright (c) RapidSoft. All rights reserved.
//
//

using System.Data.Common;
using ALE.ETLBox.ConnectionManager;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;

namespace EtlBox.Database.Tests.Infrastructure.Containers
{
    public sealed class SqlServerContainerManager : IContainerManager
    {
        private const string _user = "sa";
        private const string _password = "QWEqaz123!";
        private string _database = "master";

        public SqlServerContainerManager()
        {
            Container = new ContainerBuilder()
                .WithImage("mcr.microsoft.com/azure-sql-edge:1.0.7")
                .WithEnvironment("ACCEPT_EULA", "Y")
                .WithEnvironment("MSSQL_SA_PASSWORD", _password)
                .WithEnvironment("MSSQL_PID", "Developer")
                .WithEnvironment("MSSQL_TELEMETRY_ENABLED", "FALSE")
                //.WithEnvironment("MSSQL_COLLATION", "Cyrillic_General_CI_AS")
                .WithPortBinding(1433, true)
                .WithWaitStrategy(
                    Wait.ForUnixContainer()
                        .UntilOperationIsSucceeded(() => CheckSqlServerConnectivity(GetConnectionString()), 100)
                )
                .WithAutoRemove(true)
                .WithCleanUp(true)
                .Build();
        }

        public IContainer Container { get; private set; }

        public ConnectionManagerType Provider => ConnectionManagerType.SqlServer;

        public ConnectionManagerType ConnectionType => ConnectionManagerType.SqlServer;

        public string User { get; set; } = null!;

        public string Password { get; set; } = null!;

        public string GetConnectionString() => GetConnectionBuilder().ConnectionString;

        public DbConnectionStringBuilder GetConnectionBuilder()
        {
            var sqlConnectionStringBuilder = new SqlConnectionStringBuilder();
            sqlConnectionStringBuilder.DataSource = $"{Container.Hostname},{Container.GetMappedPublicPort(1433)}";
            sqlConnectionStringBuilder.UserID = User ?? _user;
            sqlConnectionStringBuilder.Password = Password ?? _password;
            sqlConnectionStringBuilder.InitialCatalog = _database;
            sqlConnectionStringBuilder.IntegratedSecurity = false;
            sqlConnectionStringBuilder.TrustServerCertificate = true;

            Console.WriteLine($"Connection: {sqlConnectionStringBuilder.ConnectionString}");
            return sqlConnectionStringBuilder;
        }

        public void SetDatabase(string database)
        {
            Console.WriteLine($"Set database: {database}");
            _database = database;
        }

        public async Task StartAsync()
        {
            await Container.StartAsync().ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            await Container.DisposeAsync().ConfigureAwait(false);
        }

        private bool CheckSqlServerConnectivity(string sqlConnectionString)
        {
            try
            {
                using var conn = new SqlConnection(sqlConnectionString);
                conn.Open();
                using var cmd = new SqlCommand("select 1", conn);
                var result = cmd.ExecuteScalar();
                return result != null && result.ToString() == "1";
            }
            catch (Exception ex) when (ex is SqlException or InvalidOperationException)
            {
                return false;
            }
        }

        public void DropDatabase(string database)
        {
            throw new NotImplementedException();
        }

        public void ExecuteCommand(string sql)
        {
            using var conn = new SqlConnection(GetConnectionString());
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public IConnectionManager GetConnectionManager()
        {
            var cs = GetConnectionString();
            return new SqlConnectionManager(cs);
        }

        public void SetDatabase(string database, string user = null, string password = null)
        {
            _database = database;
            User = user;
            Password = password;
        }

        public void UseDefaults()
        {
            _database = "master";
            User = null!;
            Password = null!;
        }

        public void CreateDatabase(string database)
        {
            ExecuteCommand($@"create database [{database}]");
            _database = database;
        }
    }
}
