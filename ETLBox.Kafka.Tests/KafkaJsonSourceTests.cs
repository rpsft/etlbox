using System.Diagnostics;
using System.Dynamic;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.DataFlow;
using Confluent.Kafka;
using ETLBox.Primitives;
using JetBrains.Annotations;
using Moq;
using Xunit.Abstractions;
using CancellationTokenSource = System.Threading.CancellationTokenSource;

namespace ETLBox.Kafka.Tests;

public class KafkaJsonSourceTests : IClassFixture<KafkaContainerFixture>
{
    private readonly KafkaContainerFixture _fixture;
    private readonly ITestOutputHelper _output;

    private string BootstrapAddress => _fixture.BootstrapAddress;

    private string TopicName { get; } = $"test-{Guid.NewGuid()}";

    private ConsumerConfig GetConsumerConfig(bool enablePartitionEof)
    {
        return new ConsumerConfig
        {
            BootstrapServers = BootstrapAddress,
            GroupId = "test-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = enablePartitionEof
        };
    }

    public KafkaJsonSourceTests(KafkaContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
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
    public void ShouldReadDynamicObject()
    {
        // Arrange
        const string jsonString = "{\"name\":\"test\"}";
        ProduceJson(jsonString);
        var (block, target) = SetupMockTarget();
        target.Setup(t => t.TargetBlock).Returns(block.Object);
        var kafkaSource = new KafkaJsonSource<ExpandoObject>
        {
            ConsumerConfig = GetConsumerConfig(true),
            Topic = TopicName,
        };
        kafkaSource.LinkTo(target.Object);
        // Act
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

    [UsedImplicitly]
    public record TestRecord(
        string Name,
        int IntValue,
        bool BoolValue,
        DateTime DateValue,
        string CamelCase
    );

    [Fact]
    public void ShouldReadTypedObject()
    {
        // Arrange
        const string jsonString =
            @"{
                ""name"":""test"",
                ""intValue"": 1, 
                ""boolValue"": true,
                ""dateValue"": ""2021-01-01T00:00:00"", 
                ""camelCase"": ""test""
            }";
        ProduceJson(jsonString);
        var block = new Mock<ITargetBlock<TestRecord>>();
        var target = new Mock<IDataFlowDestination<TestRecord>>();
        target.Setup(t => t.TargetBlock).Returns(block.Object);
        var kafkaSource = new KafkaJsonSource<TestRecord>
        {
            ConsumerConfig = GetConsumerConfig(true),
            Topic = TopicName,
            JsonSerializerOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            }
        };
        kafkaSource.LinkTo(target.Object);
        // Act
        kafkaSource.Execute();
        // Assert
        block.Verify(
            b =>
                b.OfferMessage(
                    It.IsAny<DataflowMessageHeader>(),
                    It.Is(
                        new TestRecord(
                            "test",
                            1,
                            true,
                            new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Local),
                            "test"
                        ),
                        EqualityComparer<TestRecord>.Default
                    ),
                    It.IsAny<ISourceBlock<TestRecord>>(),
                    It.IsAny<bool>()
                )
        );
    }

    [Fact]
    public void ShouldReadMultipleObjects()
    {
        // Arrange
        var (block, target) = SetupMockTarget();
        var kafkaSource = new KafkaJsonSource<ExpandoObject>
        {
            ConsumerConfig = GetConsumerConfig(true),
            Topic = TopicName,
        };
        kafkaSource.LinkTo(target.Object);
        // Act
        for (var i = 0; i < 10; i++)
        {
            ProduceJson($"{{\"name\":\"test{i}\"}}");
        }
        kafkaSource.Execute();
        target.Object.Wait();
        // Assert
        block.Verify(
            b =>
                b.OfferMessage(
                    It.IsAny<DataflowMessageHeader>(),
                    It.IsAny<ExpandoObject>(),
                    It.IsAny<ISourceBlock<ExpandoObject>>(),
                    It.IsAny<bool>()
                ),
            Times.Exactly(10)
        );
    }

    [Fact]
    public async Task ShouldReadContinuously()
    {
        // Arrange
        ProduceJson("{\"name\":\"test0\"}"); // Add first message synchronously to create topic
        var generator = Task.Run(async () =>
        {
            for (var i = 1; i < 10; i++)
            {
                await Task.Delay(100, CancellationToken.None);
                ProduceJson($"{{\"name\":\"test{i}\"}}");
                _output.WriteLine($"Produced test{i}");
            }
        });
        var (block, target) = SetupMockTarget();
        var kafkaSource = new KafkaJsonSource<ExpandoObject>
        {
            ConsumerConfig = GetConsumerConfig(false),
            Topic = TopicName
        };
        kafkaSource.LinkTo(target.Object);

        // Act
        using var timeoutSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(2000));
        var timer = Stopwatch.StartNew();
        var executeTask = kafkaSource.ExecuteAsync(timeoutSource.Token);
        var destinationTask = target.Object.Completion;

        // Assert
        _output.WriteLine("Waiting for completion...");
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => Task.WhenAll(executeTask, generator, destinationTask)
        );
        timer.Stop();
        // Should take longer than timeout (which is more than generation time)
        Assert.InRange(timer.ElapsedMilliseconds, 1500, 300000);
        block.Verify(
            b =>
                b.OfferMessage(
                    It.IsAny<DataflowMessageHeader>(),
                    It.IsAny<ExpandoObject>(),
                    It.IsAny<ISourceBlock<ExpandoObject>>(),
                    It.IsAny<bool>()
                ),
            Times.Exactly(10)
        );
    }

    private (
        Mock<ITargetBlock<ExpandoObject>> block,
        Mock<IDataFlowDestination<ExpandoObject>> target
    ) SetupMockTarget()
    {
        var block = new Mock<ITargetBlock<ExpandoObject>>();
        block
            .Setup(
                b =>
                    b.OfferMessage(
                        It.IsAny<DataflowMessageHeader>(),
                        It.IsAny<ExpandoObject>(),
                        It.IsAny<ISourceBlock<ExpandoObject>>(),
                        It.IsAny<bool>()
                    )
            )
            .Returns(
                (
                    DataflowMessageHeader _,
                    ExpandoObject messageValue,
                    ISourceBlock<ExpandoObject> _,
                    bool _
                ) =>
                {
                    _output.WriteLine($"Received message {messageValue}");
                    return DataflowMessageStatus.Accepted;
                }
            );
        var target = new Mock<IDataFlowDestination<ExpandoObject>>();
        target.Setup(t => t.TargetBlock).Returns(block.Object);
        return (block, target);
    }

    private IEnumerable<string> ConsumeJson()
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(GetConsumerConfig(true)).Build();
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
