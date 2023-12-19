using System.Data;
using System.Data.Common;
using ClickHouse.Ado;
using DotNet.Testcontainers.Containers;
using EtlBox.ClickHouse.ConnectionManager;
using EtlBox.ClickHouse.ConnectionStrings;
using ETLBox.Primitives;
using Testcontainers.ClickHouse;

namespace EtlBox.Database.Tests.Infrastructure.Containers
{
    public class ClickHouseContainerManager : IContainerManager
    {
        private string _database = "testDatabase";
        private string _user => "sa";
        private string _password => "QWEqaz123!";

        public IContainer Container { get; private set; }

        public ConnectionManagerType ConnectionType => ConnectionManagerType.ClickHouse;

        public ClickHouseContainerManager()
        {
            Container = new ClickHouseBuilder()
                .WithPortBinding(8123, false)
                .WithDatabase(_database)
                .WithUsername(_user)
                .WithPassword(_password)
                .WithCleanUp(true)
                .WithAutoRemove(true)
                .Build();
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
            var builder = new ClickHouseConnectionStringBuilder();
            builder.Host = Container.Hostname;
            builder.Port = Container.GetMappedPublicPort(9000);
            builder.User = _user;
            builder.Password = _password;
            builder.Database = _database;
            return builder;
        }

        public IConnectionManager GetConnectionManager()
        {
            var cs = GetConnectionString();
            return new ClickHouseConnectionManager(cs);
        }

        public void SetDatabase(string database)
        {
            _database = database;
        }

        public void DropDatabase(string database)
        {
            UseDefaults();
            ExecuteCommand($"DROP DATABASE IF EXISTS `{database}` FORCE");
        }

        public void UseDefaults()
        {
            _database = "default";
        }

        private IDbConnection GetConnection()
            => new ClickHouseConnection(GetConnectionString());

        public void CreateDatabase(string database)
        {
            ExecuteCommand($"create database `{database}`");
            _database = database;
        }
    }
}
