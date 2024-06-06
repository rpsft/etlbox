using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit.Abstractions;

namespace ETLBox.RabbitMq.Tests;

[Collection(nameof(RabbitMqCollection))]
public sealed class RabbitMqContainerTest : RabbitMqTestBase
{
    public RabbitMqContainerTest(RabbitMqFixture fixture, ITestOutputHelper logger) : base(fixture, logger)
    {
    }

    [Fact]
    public void ConsumeMessageFromQueue()
    {
        const string queue = "hello";

        const string message = "Hello World!";

        string? actualMessage = null;

        // Signal the completion of message reception.
        EventWaitHandle waitHandle = new ManualResetEvent(false);

        using var connection = Fixture.GetConnectionFactory().CreateConnection();

        // Send a message to the channel.
        using var channelToConsume = connection.CreateModel();
        channelToConsume.QueueDeclare(queue, false, false, false, null);

        using var channelToPublish = connection.CreateModel();
        
        channelToPublish.BasicPublish(string.Empty, queue, null, Encoding.Default.GetBytes(message));

        // Consume a message from the channel.
        var consumer = new EventingBasicConsumer(channelToConsume);
        consumer.Received += (_, eventArgs) =>
        {
            actualMessage = Encoding.Default.GetString(eventArgs.Body.ToArray());
            waitHandle.Set();
        };

        channelToConsume.BasicConsume(queue, true, consumer);
        waitHandle.WaitOne(TimeSpan.FromSeconds(1));

        Assert.Equal(message, actualMessage);
    }
}
