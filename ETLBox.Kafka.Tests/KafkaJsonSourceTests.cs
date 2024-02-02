using System.Diagnostics;
using System.Dynamic;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.Common.ControlFlow;
using ALE.ETLBox.DataFlow;
using Confluent.Kafka;
using ETLBox.Primitives;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit.Abstractions;
using CancellationTokenSource = System.Threading.CancellationTokenSource;

namespace ETLBox.Kafka.Tests;

public class KafkaJsonSourceTests : IClassFixture<KafkaFixture>
{
    private readonly KafkaFixture _fixture;
    private readonly ITestOutputHelper _output;

    private string TopicName { get; } = $"test-{Guid.NewGuid()}";

    private ConsumerConfig GetConsumerConfig(bool enablePartitionEof, string? topicName = null)
    {
        return new ConsumerConfig
        {
            BootstrapServers = _fixture.BootstrapAddress,
            GroupId = $"{topicName ?? TopicName}-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = enablePartitionEof
        };
    }

    public KafkaJsonSourceTests(KafkaFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        ControlFlow.LoggerFactory = new LoggerFactory(new[] { new TestOutputLoggerProvider(_output) });
    }

    [Fact]
    public void ShouldProduceAndConsumeDirectlyToKafka()
    {
        // Arrange
        const string jsonString = "{\"name\":\"test\"}";
        // Act
        ProduceJson(jsonString);
        var result = ConsumeJson(true, CancellationToken.None).ToArray();
        // Assert
        Assert.Single(result);
        Assert.Equal(jsonString, result[0]);
    }

    [Fact]
    public void ShouldProduceAndConsumeDirectlyToKafkaWithMultipleTopics()
    {
        // Arrange
        var preTopic = $"{TopicName}-pre";
        ProduceJson("{\"name\":\"direct-test-pre\"}", preTopic); // Add first message synchronously to create topic
        ProduceJson("{\"name\":\"direct-test-0\"}"); // Add first message synchronously to create topic
        ProduceJson("{\"name\":\"direct-test-1\"}");
        var timeout = TimeSpan.FromSeconds(1.0);

        // Act
        var preResults = ConsumeJson(true, CancellationToken.None, preTopic).ToList();

        var watch = new Stopwatch();
        watch.Start();
        var cancellationToken = new CancellationTokenSource(timeout).Token;
        var results = ConsumeJson(false, cancellationToken).ToList();

        watch.Stop();

        // Assert
        Assert.InRange(watch.ElapsedMilliseconds, timeout.TotalMilliseconds, 300000);
        Assert.Single(preResults);
        Assert.Equal(2, results.Count);
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
    public void ShouldReadContinuously()
    {
        // Arrange
        ProduceJson("{\"name\":\"test0\"}"); // Add first message synchronously to create topic
        var generator = Task.Run(async () =>
        {
            for (var i = 1; i < 10; i++)
            {
                await Task.Delay(100, CancellationToken.None);
                ProduceJson($"{{\"name\":\"test{i}\"}}");
                _output.WriteLine($"Produced test {i} to topic {TopicName}");
            }
        });
        var (block, target) = SetupMockTarget();
        var kafkaSource = new TestKafkaJsonSource(_output)
        {
            ConsumerConfig = GetConsumerConfig(false),
            Topic = TopicName
        };
        kafkaSource.LinkTo(target.Object);

        // Act
        using var timeoutSource = new CancellationTokenSource(TimeSpan.FromMilliseconds(3000));
        var timer = Stopwatch.StartNew();
        var executeTask = kafkaSource.ExecuteAsync(timeoutSource.Token);
        var destinationTask = target.Object.Completion;

        // Assert
        _output.WriteLine("Waiting for completion...");

        var e = Record.Exception(() => Task.WaitAll(executeTask, generator, destinationTask));

        Assert.Multiple(
            () => Assert.IsType<TaskCanceledException>((e as AggregateException)?.InnerException),
            () => Assert.Equal(TaskStatus.Canceled, executeTask.Status),
            () => Assert.Null(generator.Exception),
            () => Assert.Null(destinationTask.Exception)
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

    private IEnumerable<string> ConsumeJson(
        bool enablePartitionEof,
        CancellationToken cancellationToken,
        string? topicName = null
    )
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(
            GetConsumerConfig(enablePartitionEof, topicName)
        ).Build();
        _output.WriteLine($"Subscribing to topic {topicName ?? TopicName}...");
        consumer.Subscribe(topicName ?? TopicName);
        while (true)
        {
            ConsumeResult<Ignore, string> consumeResult;
            try
            {
                consumeResult = consumer.Consume(cancellationToken);
                _output.WriteLine(
                    $"Consumed direct message {consumeResult.Message?.Value ?? "null"}"
                );
            }
            catch (OperationCanceledException)
            {
                break;
            }
            if (consumeResult.IsPartitionEOF || consumeResult.Message is null)
            {
                break;
            }

            yield return consumeResult.Message.Value;
        }
    }

    private void ProduceJson(string jsonString, string? topicName = null)
    {
        var config = new ProducerConfig { BootstrapServers = _fixture.BootstrapAddress };
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        var message = new Message<Null, string> { Value = jsonString };
        _output.WriteLine(
            $"Producing message {message.Value} to topic {topicName ?? TopicName}..."
        );
        producer.Produce(topicName ?? TopicName, message);
        producer.Flush();
    }
}

public class TestKafkaJsonSource : KafkaJsonSource<ExpandoObject>
{
    private readonly ITestOutputHelper _output;

    public TestKafkaJsonSource(ITestOutputHelper output)
    {
        _output = output;
    }

    protected override ExpandoObject? ConvertToOutputValue(
        string kafkaValue,
        Action<Exception, string>? logRowOnError = null
    )
    {
        _output.WriteLine($"Converting message {kafkaValue}");
        return base.ConvertToOutputValue(kafkaValue, logRowOnError);
    }
}
