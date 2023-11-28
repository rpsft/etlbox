using System.Data;
using ALE.ETLBox.ConnectionManager;
using ClickHouse.Ado;
using DotNet.Testcontainers.Containers;
using EtlBox.ClickHouse.ConnectionManager;
using EtlBox.ClickHouse.ConnectionStrings;
using Testcontainers.ClickHouse;

namespace EtlBox.Database.Tests.Containers
{
    public class ClickHouseContainerManager : IContainerManager
    {
        private string _database = "testDatabase";
        private string _user => "sa";
        private string _password => "QWEqaz123!";

        public IContainer Container { get; private set; }

        public ConnectionManagerType ConnectionType => ConnectionManagerType.ClickHouse;

        public string User { get; set; } = null!;

        public string Password { get; set; } = null!;

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

        public ClickHouseConnectionStringBuilder GetConnectionBuilder()
        {
            var builder = new ClickHouseConnectionStringBuilder();
            builder.Host = Container.Hostname;
            builder.Port = Container.GetMappedPublicPort(9000);
            builder.User = User ?? _user;
            builder.Password = Password ?? _password;
            builder.Database = _database;
            return builder;
        }

        public IConnectionManager GetConnectionManager()
        {
            var cs = GetConnectionString();
            return new ClickHouseConnectionManager(cs);
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
            _database = "default";
            User = null!;
            Password = null!;
        }

        private IDbConnection GetConnection()
            => new ClickHouseConnection(GetConnectionString());
    }
}
