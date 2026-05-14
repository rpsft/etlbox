using ALE.ETLBox.Common.ControlFlow;
using ALE.ETLBox.DataFlow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Moq;

namespace ETLBox.Kafka.Tests;

public class KafkaTransformationDeliveryHandlerTests
{
    private class TestableKafkaTransformation : KafkaTransformation<string, string>
    {
        public TestableKafkaTransformation(IProducer<Null, string> producer)
            : base(producer) { }

        protected override string BuildMessageValue(string input) => input;
    }

    [Fact]
    public void ShouldLogError_WhenDeliveryReportHasError()
    {
        // Arrange
        var mockLogger = new Mock<ILogger>();
        mockLogger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        var mockFactory = new Mock<ILoggerFactory>();
        mockFactory.Setup(f => f.CreateLogger(It.IsAny<string>())).Returns(mockLogger.Object);
        ControlFlow.LoggerFactory = mockFactory.Object;

        var capturedHandlers = new List<Action<DeliveryReport<Null, string>>>();
        var mockProducer = new Mock<IProducer<Null, string>>();
        mockProducer
            .Setup(p =>
                p.Produce(
                    It.IsAny<string>(),
                    It.IsAny<Message<Null, string>>(),
                    It.IsAny<Action<DeliveryReport<Null, string>>>()
                )
            )
            .Callback<string, Message<Null, string>, Action<DeliveryReport<Null, string>>>(
                (_, _, handler) => capturedHandlers.Add(handler)
            );

        var transformation = new TestableKafkaTransformation(mockProducer.Object)
        {
            TopicName = "test-topic",
        };
        var source = new MemorySource<string>(new[] { "test-value" });
        var dest = new MemoryDestination<string?>();
        source.LinkTo(transformation);
        transformation.LinkTo(dest);
        source.Execute();
        dest.Wait();

        Assert.Single(capturedHandlers);

        // Act: Kafka сообщает об ошибке доставки
        capturedHandlers[0]
            (
                new DeliveryReport<Null, string>
                {
                    Error = new Error(ErrorCode.BrokerNotAvailable, "Broker not available"),
                    Message = new Message<Null, string> { Value = "test-value" },
                }
            );

        // Assert
        mockLogger.Verify(
            x =>
                x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>(
                        (v, _) =>
                            v.ToString()!.Contains("test-value")
                            && v.ToString()!.Contains("Broker not available")
                    ),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()
                ),
            Times.Once
        );
    }

    [Fact]
    public void ShouldNotLogError_WhenDeliveryReportSucceeds()
    {
        // Arrange
        var mockLogger = new Mock<ILogger>();
        mockLogger.Setup(x => x.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        var mockFactory = new Mock<ILoggerFactory>();
        mockFactory.Setup(f => f.CreateLogger(It.IsAny<string>())).Returns(mockLogger.Object);
        ControlFlow.LoggerFactory = mockFactory.Object;

        var capturedHandlers = new List<Action<DeliveryReport<Null, string>>>();
        var mockProducer = new Mock<IProducer<Null, string>>();
        mockProducer
            .Setup(p =>
                p.Produce(
                    It.IsAny<string>(),
                    It.IsAny<Message<Null, string>>(),
                    It.IsAny<Action<DeliveryReport<Null, string>>>()
                )
            )
            .Callback<string, Message<Null, string>, Action<DeliveryReport<Null, string>>>(
                (_, _, handler) => capturedHandlers.Add(handler)
            );

        var transformation = new TestableKafkaTransformation(mockProducer.Object)
        {
            TopicName = "test-topic",
        };
        var source = new MemorySource<string>(new[] { "test-value" });
        var dest = new MemoryDestination<string?>();
        source.LinkTo(transformation);
        transformation.LinkTo(dest);
        source.Execute();
        dest.Wait();

        Assert.Single(capturedHandlers);

        // Act: Kafka подтверждает успешную доставку
        capturedHandlers[0]
            (
                new DeliveryReport<Null, string>
                {
                    Error = new Error(ErrorCode.NoError),
                    Message = new Message<Null, string> { Value = "test-value" },
                }
            );

        // Assert
        mockLogger.Verify(
            x =>
                x.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception?>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()
                ),
            Times.Never
        );
    }
}
