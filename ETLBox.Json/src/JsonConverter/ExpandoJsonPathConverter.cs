using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace ETLBox.Json
{
    public class JsonProperty2JsonPath
    {
        public string JsonPropertyName { get; set; }
        public string JsonPath { get; set; }
        public string NewPropertyName
        {
            get => _newPropertyName ?? JsonPropertyName;
            set => _newPropertyName = value;
        }
        private string _newPropertyName;
        public bool Validate()
        {
            if (string.IsNullOrWhiteSpace(JsonPropertyName) || string.IsNullOrWhiteSpace(JsonPath) || string.IsNullOrWhiteSpace(NewPropertyName))
                return false;
            return true;
        }
    }
    /// <summary>
    /// Allows to pass JsonPath string that are applied for particular property names - this will work one on the first level
    /// of the
    /// </summary>
    /// <remarks>
    /// https://github.com/JamesNK/Newtonsoft.Json/blob/master/Src/Newtonsoft.Json/Converters/ExpandoObjectConverter.cs
    /// </remarks>
    public class ExpandoJsonPathConverter : JsonConverter
    {
        public IEnumerable<JsonProperty2JsonPath> PathLookups { get; set; }
        public ExpandoJsonPathConverter(IEnumerable<JsonProperty2JsonPath> pathLookups)
        {
            PathLookups = pathLookups;
        }

        /// <inheritdoc />
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            // can write is set to false
        }

        /// <inheritdoc />
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return ReadValue(reader);
        }

        private object ReadValue(JsonReader reader)
        {
            switch (reader.TokenType)
            {
                case JsonToken.StartObject:
                    return ReadObject(reader);
                default:
                    if (IsPrimitiveToken(reader.TokenType))
                    {
                        return reader.Value;
                    }
                    reader.Skip();
                    return null;
            }
        }

        private object ReadObject(JsonReader reader)
        {
            IDictionary<string, object> expandoObject = new ExpandoObject();

            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.PropertyName:
                        string propertyName = reader.Value?.ToString();

                        if (!reader.Read())
                            throw new JsonSerializationException("Unexpected end when reading ExpandoObject.");

                        if (IsPrimitiveToken(reader.TokenType))
                        {
                            expandoObject[propertyName] = reader.Value;
                        }
                        else
                        {
                            var jo = JContainer.Load(reader);
                            foreach (var pl in PathLookups.Where(l => l.JsonPropertyName == propertyName))
                            {
                                if (pl?.Validate() ?? false)
                                    expandoObject[pl.NewPropertyName] = GetValueFromJsonPath(jo, pl.JsonPath);
                            }
                        }
                        break;
                    case JsonToken.Comment:
                        break;
                    case JsonToken.EndObject:
                        return expandoObject;
                }
            }

            throw new JsonSerializationException("Unexpected end when reading ExpandoObject.");
        }

        private object GetValueFromJsonPath(JToken jo, string path)
        {
            object val = null;
            JToken t = jo.SelectToken(path);
            if (t is JValue)
                val = ((JValue)t).Value;
            return val;
        }

        /// <inheritdoc />
        public override bool CanConvert(Type objectType)
        {
            return (objectType == typeof(ExpandoObject));
        }

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// https://github.com/JamesNK/Newtonsoft.Json/blob/master/Src/Newtonsoft.Json/Utilities/JsonTokenUtils.cs
        private bool IsPrimitiveToken(JsonToken token)
        {
            switch (token)
            {
                case JsonToken.Integer:
                case JsonToken.Float:
                case JsonToken.String:
                case JsonToken.Boolean:
                case JsonToken.Undefined:
                case JsonToken.Null:
                case JsonToken.Date:
                case JsonToken.Bytes:
                    return true;
                default:
                    return false;
            }
        }
    }
}
