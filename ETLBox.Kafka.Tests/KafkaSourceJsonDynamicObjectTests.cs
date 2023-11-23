using System.Collections;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.DataFlow;
using Confluent.Kafka;
using ETLBox.Kafka.Tests.Utilities;
using Moq;

namespace ETLBox.Kafka.Tests;

public class KafkaSourceJsonDynamicObjectTests : IClassFixture<KafkaContainerFixture>
{
    private readonly KafkaContainerFixture _fixture;

    private string BootstrapAddress => _fixture.BootstrapAddress;
    private string TopicName { get; } = $"test-{Guid.NewGuid()}";

    private ConsumerConfig ConsumerConfig =>
        new()
        {
            BootstrapServers = BootstrapAddress,
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        };

    public KafkaSourceJsonDynamicObjectTests(KafkaContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void ShouldProduceAndConsumeDirectlyToKafka()
    {
        // Arrange
        const string jsonString = "{\"name\":\"test\"}";
        // Act
        ProduceJson(jsonString);
        var result = ConsumeJson().ToArray();
        // Assert
        Assert.Single(result);
        Assert.Equal(jsonString, result[0]);
    }

    [Fact]
    public void ShouldConsumeJsonWithKafkaSource()
    {
        // Arrange
        const string jsonString = "{\"name\":\"test\"}";
        ProduceJson(jsonString);
        var block = new Mock<ITargetBlock<ExpandoObject>>();
        var target = new Mock<IDataFlowDestination<ExpandoObject>>();
        target.Setup(t => t.TargetBlock).Returns(block.Object);
        // Act
        var kafkaSource = new KafkaJsonSource<ExpandoObject>()
        {
            ConsumerConfig = ConsumerConfig,
            Topic = TopicName,
        };
        kafkaSource.LinkTo(target.Object);
        kafkaSource.Execute();
        // Assert
        block.Verify(
            b =>
                b.OfferMessage(
                    It.IsAny<DataflowMessageHeader>(),
                    It.Is<ExpandoObject>(
                        message =>
                            ((IDictionary<string, object?>)message)["name"] as string == "test"
                    ),
                    It.IsAny<ISourceBlock<ExpandoObject>>(),
                    It.IsAny<bool>()
                )
        );
    }

    private IEnumerable<string> ConsumeJson()
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(ConsumerConfig).Build();
        consumer.Subscribe(TopicName);
        while (true)
        {
            var consumeResult = consumer.Consume();
            if (consumeResult.IsPartitionEOF)
            {
                break;
            }

            yield return consumeResult.Message.Value;
        }
    }

    private void ProduceJson(string jsonString)
    {
        var config = new ProducerConfig { BootstrapServers = BootstrapAddress };
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        var message = new Message<Null, string> { Value = jsonString };
        producer.Produce(TopicName, message);
        producer.Flush();
    }
}
