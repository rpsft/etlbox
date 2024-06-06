using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace ETLBox.RabbitMq.Tests;

public sealed class TestOutputLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _logger;

    public TestOutputLoggerProvider(ITestOutputHelper logger)
    {
        _logger = logger;
    }


    public ILogger CreateLogger(string categoryName)
    {
        return new ConsoleLogger(_logger);
    }

    public void Dispose()
    {
    }
}

internal class ConsoleLogger : ILogger
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
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        _logger.WriteLine($"{logLevel}, {eventId}, {state}: {formatter(state, exception)}");
    }
}
