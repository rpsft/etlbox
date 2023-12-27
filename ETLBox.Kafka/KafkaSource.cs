using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.Common.DataFlow;
using Confluent.Kafka;
using ETLBox.Primitives;
using JetBrains.Annotations;

namespace ALE.ETLBox.DataFlow;

/// <summary>
/// Kafka generic source
/// </summary>
/// <typeparam name="TOutput">Result type</typeparam>
/// <typeparam name="TKafkaValue"></typeparam>
[PublicAPI]
public abstract class KafkaSource<TOutput, TKafkaValue> : DataFlowSource<TOutput>, IDataFlowSource<TOutput>
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

    /// <summary>
    /// Override this method to convert the Kafka value to the output value
    /// </summary>
    /// <param name="kafkaValue">value as read from Kafka consumer</param>
    /// <returns>Output value to put to destination</returns>
    protected abstract TOutput ConvertToOutputValue(TKafkaValue kafkaValue);

    /// <summary>
    /// Main execution method
    /// </summary>
    /// <param name="cancellationToken"></param>
    public override void Execute(CancellationToken cancellationToken)
    {
        LogStart();

        var builder = new ConsumerBuilder<Ignore, TKafkaValue>(ConsumerConfig);
        ConfigureConsumerBuilder?.Invoke(builder);
        using var consumer = builder.Build();

        consumer.Subscribe(Topic);

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!ConsumeAndSendSingleMessage(consumer, cancellationToken))
            {
                break;
            }
        }
        Buffer.Complete();
        LogFinish();
    }

    /// <summary>
    /// Consume, convert and send a single message
    /// </summary>
    /// <param name="consumer"></param>
    /// <param name="cancellationToken"></param>
    /// <returns>false to exit reading cycle, true to continue</returns>
    private bool ConsumeAndSendSingleMessage(IConsumer<Ignore, TKafkaValue> consumer, CancellationToken cancellationToken)
    {
        TKafkaValue? kafkaValue = default;
        try
        {
            var consumeResult = consumer.Consume(cancellationToken);
            if (consumeResult == null)
            {
                return !cancellationToken.IsCancellationRequested;
            }

            if (consumeResult.IsPartitionEOF)
            {
                return false;
            }

            kafkaValue = consumeResult.Message.Value;
            if (kafkaValue is null)
            {
                // Nulls do not propagate through the pipeline
                return true;
            }

            var outputValue = ConvertToOutputValue(kafkaValue);
            Buffer.SendAsync(outputValue, cancellationToken).Wait(cancellationToken);
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

        return true;
    }
}
