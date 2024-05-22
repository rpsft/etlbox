using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ALE.ETLBox.Common.ControlFlow;
using ALE.ETLBox.DataFlow;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace ETLBox.Kafka.Tests
{
    public partial class KafkaTransformationTests : IClassFixture<KafkaFixture>
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

        public KafkaTransformationTests(KafkaFixture fixture, ITestOutputHelper output)
        {
            _fixture = fixture;
            _output = output;
            ControlFlow.LoggerFactory = new LoggerFactory(
                new[] { new TestOutputLoggerProvider(_output) }
            );
        }

        [Fact]
        public async void ShouldProduceAndConsumeDirectlyToKafka()
        {
            // Arrange
            dynamic data = new ExpandoObject();
            data.TestName = "Tom";

            var config = new ProducerConfig { BootstrapServers = _fixture.BootstrapAddress };
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            var transformation = new KafkaTransformation(producer)
            {
                MessageTemplate = "{\"NewMessage\": {\"TestValue\":\"{{TestName}}\"}}",
                TopicName = TopicName
            };

            var source = new MemorySource<ExpandoObject>(new ExpandoObject[] { data });
                        
            source.LinkTo(transformation);

            //Act
            source.Execute();

            await Task.Delay(10000);

            var result = ConsumeJson(true, CancellationToken.None).ToArray();


            // Assert
            Assert.Single(result);

            Assert.Equal("{\"NewMessage\": {\"TestValue\":\"Tom\"}}", result.First());
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
    }
}
