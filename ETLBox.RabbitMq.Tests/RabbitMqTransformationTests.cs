using System.Dynamic;
using System.Text;
using ALE.ETLBox.DataFlow;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit.Abstractions;

namespace ETLBox.RabbitMq.Tests
{
    [Collection(nameof(RabbitMqCollection))]
    public class RabbitMqTransformationTests : RabbitMqTestBase
    {
        private string Queue { get; } = $"test-{Guid.NewGuid()}";

        public RabbitMqTransformationTests(RabbitMqFixture fixture, ITestOutputHelper logger) : base(fixture, logger)
        {
        }

        [Fact]
        public void ShouldPublishAndConsume()
        {
            // Arrange
            dynamic data = new ExpandoObject();
            data.TestName = "Tom";

            // Signal the completion of message reception.
            EventWaitHandle waitHandle = new ManualResetEvent(false);

            using var connection = Fixture.GetConnectionFactory().CreateConnection();

            // Send a message to the channel.
            using var channelToConsume = connection.CreateModel();
            channelToConsume.QueueDeclare(Queue, false, false, false, null);

            var transformation = new RabbitMqTransformation(Fixture.GetConnectionFactory())
            {
                MessageTemplate = "{\"NewMessage\": {\"TestValue\":\"{{TestName}}\"}}",
                Queue = Queue
            };

            var source = new MemorySource<ExpandoObject>(new ExpandoObject[] { data });
            var dest = new MemoryDestination<ExpandoObject?>();

            //Act
            source.LinkTo(transformation);
            transformation.LinkTo(dest);
            source.Execute();
            dest.Wait();

            string? actualMessage = null;

            // Consume a message from the channel.
            var consumer = new EventingBasicConsumer(channelToConsume);
            consumer.Received += (_, eventArgs) =>
            {
                actualMessage = Encoding.Default.GetString(eventArgs.Body.ToArray());
                waitHandle.Set();
            };

            channelToConsume.BasicConsume(Queue, true, consumer);
            waitHandle.WaitOne(TimeSpan.FromSeconds(1));

            // Assert
            Assert.NotNull(actualMessage);

            Assert.Equal("{\"NewMessage\": {\"TestValue\":\"Tom\"}}", actualMessage);
        }
    }
}
