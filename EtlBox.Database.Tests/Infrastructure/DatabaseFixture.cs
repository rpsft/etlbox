using EtlBox.Database.Tests.Infrastructure.Containers;
using ETLBox.Primitives;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.Infrastructure
{
    [UsedImplicitly]
    public class DatabaseFixture : IAsyncLifetime
    {
        private ClickHouseContainerManager _clickHouse = null!;
        private SqlServerContainerManager _sqlServer = null!;
        private PostgresContainerManager _postgres = null!;

        public async Task InitializeAsync()
        {
            _clickHouse = new ClickHouseContainerManager();
            _sqlServer = new SqlServerContainerManager();
            _postgres = new PostgresContainerManager();
            var t1 = _clickHouse.StartAsync();
            var t2 = _sqlServer.StartAsync();
            await _postgres.StartAsync().ConfigureAwait(false);
            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);
        }

        public IContainerManager GetContainer(ConnectionManagerType provider)
        {
            IContainerManager container = provider switch
            {
                ConnectionManagerType.ClickHouse => _clickHouse,
                ConnectionManagerType.SqlServer => _sqlServer,
                ConnectionManagerType.Postgres => _postgres,
                _ => throw new NotImplementedException($"Provider '{provider}' is not implemented"),
            };
            return container;
        }

        public IConnectionManager GetConnectionManager(ConnectionManagerType provider)
            => GetContainer(provider).GetConnectionManager();

        public string QB(ConnectionManagerType provider) => GetConnectionManager(provider).QB;

        public string QE(ConnectionManagerType provider) => GetConnectionManager(provider).QE;

        public async Task DisposeAsync()
        {
            var t1 = _clickHouse.DisposeAsync();
            var t2 = _postgres.DisposeAsync();
            await _sqlServer.DisposeAsync().ConfigureAwait(false);
            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);
        }
    }

    class ConsoleLogger : ILogger
    {
        private readonly ITestOutputHelper _logger;

        public ConsoleLogger(ITestOutputHelper logger)
        {
            _logger = logger;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            throw new NotImplementedException();
        }

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        )
        {
            _logger.WriteLine($"{logLevel}, {eventId}, {state}: {formatter(state, exception)}");
        }
    }
}
