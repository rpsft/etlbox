using System;
using System.Text.Json;

namespace ALE.ETLBox.DataFlow;

/// <summary>
/// Kafka source for JSON value types
/// </summary>
/// <typeparam name="TOutput"></typeparam>
public class KafkaJsonSource<TOutput> : KafkaSource<TOutput, string>
{
    /// <summary>
    /// Options for Json deserialization
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    private readonly ExpandoObjectConverter _converter = new();

    public KafkaJsonSource()
    {
        JsonSerializerOptions = new JsonSerializerOptions()
        {
            Converters =
            {
                _converter
            }
        };
    }

    protected override TOutput ConvertToOutputValue(string kafkaValue)
    {
        var jsonSerializerOptions = JsonSerializerOptions ?? new JsonSerializerOptions();
        if (!jsonSerializerOptions.Converters.Contains(_converter))
            jsonSerializerOptions.Converters.Add(_converter);

        var outputValue =
            (TOutput?)
            JsonSerializer.Deserialize(
                kafkaValue.ToString(),
                typeof(TOutput),
                jsonSerializerOptions
            ) ?? throw new InvalidOperationException();
        return outputValue;
    }

}
