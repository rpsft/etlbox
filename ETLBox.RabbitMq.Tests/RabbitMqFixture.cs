using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace ETLBox.RabbitMq.Tests;

public sealed class RabbitMqFixture : IAsyncLifetime
{
    //private readonly RabbitMqContainer _rabbitMqContainer = new Testcontainers.RabbitMq.RabbitMqBuilder().Build();

    public Task InitializeAsync()
    {
        return Task.CompletedTask;
        //return _rabbitMqContainer.StartAsync();
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
        //return _rabbitMqContainer.DisposeAsync().AsTask();
    }

    public string GetConnectionString()
    {
        return "amqp://guest:guest@127.0.0.1:15672/";
        //return _rabbitMqContainer.GetConnectionString();
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
