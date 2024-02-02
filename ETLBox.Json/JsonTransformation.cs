using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using JetBrains.Annotations;
using Newtonsoft.Json.Linq;

namespace ALE.ETLBox.Common.DataFlow;

/// <summary>
/// Transforms fields represented as JSON strings into a destination object using JSONPath
/// </summary>
[PublicAPI]
public sealed class JsonTransformation : RowTransformation<ExpandoObject>
{
    /// <summary>
    /// Collection of mappings from the source object to the destination object
    /// </summary>
    public Dictionary<string, Mapping> Mappings { get; set; } = new();

    /// <summary>
    /// Maps JSON properties to a destination object
    /// </summary>
    [PublicAPI]
    public sealed record Mapping
    {
        /// <summary>
        /// Default constructor for deserialization
        /// </summary>
        public Mapping() : this(string.Empty, string.Empty)
        {
        }

        /// <summary>
        /// Maps JSON properties to a destination object
        /// </summary>
        public Mapping(string Name, string Path)
        {
            this.Name = Name;
            this.Path = Path;
        }

        /// <summary>
        /// Name of the JSON field in the source object
        /// </summary>
        public string Name { get; set; } = null!;

        /// <summary>
        /// JsonPath to the value inside JSON field the source object or `null` to use the whole field
        /// </summary>
        public string? Path { get; set; }

        public void Deconstruct(out string name, out string? path)
        {
            name = Name;
            path = Path;
        }
    }

    public JsonTransformation()
    {
        TransformationFunc = TransformWithJsonPath;
    }

    private ExpandoObject TransformWithJsonPath(ExpandoObject source)
    {
        var res = new ExpandoObject();
        var access = res as IDictionary<string, object?>;
        IReadOnlyDictionary<string, JObject> parsedFields = ParseJsonFields(source);

        foreach (var key in Mappings.Keys)
        {
            access.Add(key, GetValue(source, parsedFields, Mappings[key]));
        }

        return res;
    }

    private IReadOnlyDictionary<string, JObject> ParseJsonFields(IDictionary<string, object?> source) => Mappings.Values
        .Where(m => !string.IsNullOrEmpty(m.Path))
        .Select(x => x.Name)
        .Distinct()
        .Where(key => key != null && source.ContainsKey(key))
        .ToDictionary(key => key, key => JObject.Parse(source[key]!.ToString()));

    private static object? GetValue(IDictionary<string, object?> sourceObject,
        IReadOnlyDictionary<string, JObject> parsedJsonFields,
        Mapping mapping
    )
    {
        // If no path is specified, use the whole field
        if (string.IsNullOrEmpty(mapping.Path)) return sourceObject[mapping.Name];

        // If the field is not a JSON object, return empty string
        if (!parsedJsonFields.TryGetValue(mapping.Name, out JObject? jsonObj)) return string.Empty;

        // Use JSONPath to retrieve the value
        JToken value = jsonObj.SelectToken(mapping.Path!)!;

        // Convert the value to result object
        return value.Type switch
        {
            JTokenType.Array => value.ToString(),
            JTokenType.Object => value.ToString(),
            JTokenType.Null => null,
            JTokenType.Boolean => value.ToObject<bool>(),
            JTokenType.Date => value.ToObject<DateTime>(),
            JTokenType.Integer => value.ToObject<int>(),
            JTokenType.Float => value.ToObject<double>(),
            _ => value.ToObject<string>()
        };
    }
}
