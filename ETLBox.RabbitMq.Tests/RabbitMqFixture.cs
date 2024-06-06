using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace ETLBox.RabbitMq.Tests;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    public string GetConnectionString()
    {
        return "amqp://guest:guest@127.0.0.1:15672/";
    }

    public IConnectionFactory GetConnectionFactory() => new ConnectionFactory
    {
        Uri = new Uri(GetConnectionString())
    };

    public IConnectionFactory GetConnectionFactory(string connectionString) => new ConnectionFactory
    {
        Uri = new Uri(connectionString)
    };
}
