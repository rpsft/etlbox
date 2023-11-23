#nullable enable
using System;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using JetBrains.Annotations;

namespace ALE.ETLBox.DataFlow;

/// <summary>
/// Kafka source for JSON value types
/// </summary>
/// <typeparam name="TOutput"></typeparam>
public class KafkaJsonSource<TOutput> : KafkaSource<TOutput, string> { }

/// <summary>
/// Kafka generic source
/// </summary>
/// <typeparam name="TOutput"></typeparam>
/// <typeparam name="TKafkaValue"></typeparam>
[PublicAPI]
public class KafkaSource<TOutput, TKafkaValue> : DataFlowSource<TOutput>, IDataFlowSource<TOutput>
{
    /// <summary>
    /// Kafka consumer configuration
    /// </summary>
    public ConsumerConfig ConsumerConfig { get; set; } = new();

    /// <summary>
    /// Topic name to subscribe to
    /// </summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Additional configuration for the consumer builder, before building consumer
    /// </summary>
    public Action<ConsumerBuilder<Ignore, TKafkaValue>>? ConfigureConsumerBuilder { get; set; }

    // Private stuff
    private TypeInfo TypeInfo { get; set; } = new TypeInfo(typeof(TOutput)).GatherTypeInfo();

    public override void Execute()
    {
        NLogStart();

        var builder = new ConsumerBuilder<Ignore, TKafkaValue>(ConsumerConfig);
        ConfigureConsumerBuilder?.Invoke(builder);
        using var consumer = builder.Build();

        consumer.Subscribe(Topic);

        while (true)
        {
            if (!ConsumeAndSendSingleMessage(consumer))
            {
                break;
            }
        }

        NLogFinish();
    }

    private bool ConsumeAndSendSingleMessage(IConsumer<Ignore, TKafkaValue> consumer)
    {
        TKafkaValue? kafkaValue = default;
        try
        {
            var consumeResult = consumer.Consume();
            if (consumeResult.IsPartitionEOF)
            {
                return false;
            }

            kafkaValue = consumeResult.Message.Value;
            if (kafkaValue is null)
            {
                return true;
            }

            var jsonSerializerOptions = new JsonSerializerOptions
            {
                Converters = { new ExpandoObjectConverter() }
            };

            var outputValue =
                (TOutput?)
                    JsonSerializer.Deserialize(
                        kafkaValue.ToString(),
                        typeof(TOutput),
                        jsonSerializerOptions
                    ) ?? throw new InvalidOperationException();
            Buffer.SendAsync(outputValue).Wait();
        }
        catch (Exception e)
        {
            if (!ErrorHandler.HasErrorBuffer)
                throw;
            if (e is ConsumeException ex)
                ErrorHandler.Send(
                    e,
                    $"Offset: {ex.ConsumerRecord.Offset} -- Key(base64): {Convert.ToBase64String(ex.ConsumerRecord.Message.Key)} -- Value(base64): {Convert.ToBase64String(ex.ConsumerRecord.Message.Value)}"
                );
            if (e is JsonException jex)
                ErrorHandler.Send(jex, kafkaValue?.ToString() ?? "N/A");
            else
                ErrorHandler.Send(e, "N/A");
        }

        return false;
    }
}
