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
        JsonSerializerOptions = new JsonSerializerOptions() { Converters = { _converter } };
    }

    protected override TOutput? ConvertToOutputValue(
        string kafkaValue,
        Action<Exception, string>? logRowOnError = null
    )
    {
        var jsonSerializerOptions = JsonSerializerOptions ?? new JsonSerializerOptions();
        if (!jsonSerializerOptions.Converters.Contains(_converter))
            jsonSerializerOptions.Converters.Add(_converter);

        try
        {
            return JsonSerializer.Deserialize<TOutput>(kafkaValue, jsonSerializerOptions);
        }
        catch (Exception e)
        {
            logRowOnError?.Invoke(e, kafkaValue);
            return default;
        }
    }
}
