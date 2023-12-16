// Copyright (c) RapidSoft. All rights reserved.
//
//

using System.Data;
using System.Data.Common;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using ClickHouse.Ado;
using DotNet.Testcontainers.Containers;
using Npgsql;
using Testcontainers.PostgreSql;

namespace EtlBox.Database.Tests.Infrastructure.Containers
{
    public sealed class PostgresContainerManager : IContainerManager
    {
        private string _database = "postgres";

        public IContainer Container { get; private set; }

        public ConnectionManagerType ConnectionType => ConnectionManagerType.ClickHouse;

        public string User { get; set; } = null!;

        public string Password { get; set; } = null!;

        public PostgresContainerManager()
        {
            Container = new PostgreSqlBuilder().Build();
        }

        public async Task StartAsync()
        {
            await Container.StartAsync().ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            await Container.DisposeAsync().ConfigureAwait(false);
        }

        public void ExecuteCommand(string sql)
        {
            using var connection = GetConnection();
            connection.Open();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public string GetConnectionString() => GetConnectionBuilder().ConnectionString;

        public DbConnectionStringBuilder GetConnectionBuilder()
        {
            var builder = new NpgsqlConnectionStringBuilder();
            builder.ConnectionString = (Container as PostgreSqlContainer)!.GetConnectionString();
            builder.Database = _database;
            return builder;
        }

        public IConnectionManager GetConnectionManager()
        {
            var cs = GetConnectionString();
            return new PostgresConnectionManager(cs);
        }

        public void SetDatabase(string database, string user = null!, string password = null!)
        {
            _database = database;
            User = user;
            Password = password;
        }

        public void DropDatabase(string database)
        {
            UseDefaults();
            ExecuteCommand($"DROP DATABASE IF EXISTS `{database}` FORCE");
        }

        public void UseDefaults()
        {
            _database = "postgres";
            User = null!;
            Password = null!;
        }

        private IDbConnection GetConnection()
            => new ClickHouseConnection(GetConnectionString());

        public void CreateDatabase(string database)
        {
            ExecuteCommand($@"create database if not exists ""{database}""");
            _database = database;
        }
    }
}
