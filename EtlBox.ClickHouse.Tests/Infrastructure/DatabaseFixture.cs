using ALE.ETLBox.ConnectionManager;
using EtlBox.Database.Tests.Containers;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.Infrastructure
{
    [UsedImplicitly]
    public class DatabaseFixture : IAsyncLifetime
    {
        private ClickHouseContainerManager _clickHouse = null!;

        public async Task InitializeAsync()
        {
            _clickHouse = new ClickHouseContainerManager();
            await _clickHouse.StartAsync();
        }

        public IContainerManager GetContainer(ConnectionManagerType provider)
        {
            IContainerManager container = provider switch
            {
                ConnectionManagerType.ClickHouse => _clickHouse,
                _ => throw new NotImplementedException($"Provider '{provider}' is not implemented"),
            };
            return container;
        }

        public async Task DisposeAsync()
        {
            await _clickHouse.DisposeAsync();
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
            where TState : notnull
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
            Exception exception,
            Func<TState, Exception, string> formatter
        )
        {
            _logger.WriteLine($"{logLevel}, {eventId}, {state}: {formatter(state, exception)}");
        }
    }
}
