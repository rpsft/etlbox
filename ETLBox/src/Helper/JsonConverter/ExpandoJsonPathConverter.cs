using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace ALE.ETLBox.Helper
{
    public class JsonProperty2JsonPath
    {
        public string ReplacePropertyName { get; set; }
        public string JsonPath { get; set; }
        public string NewPropertyName { get; set; }
        public bool Validate()
        {
            if (string.IsNullOrWhiteSpace(ReplacePropertyName) || string.IsNullOrWhiteSpace(JsonPath) || string.IsNullOrWhiteSpace(NewPropertyName))
                return false;
            if (!Regex.IsMatch(JsonPath, @"^[a-zA-Z0-9_.-]+$"))
                return false;
            return true;
        }
    }
    /// <summary>
    /// Allows to pass JsonPath string that are applied for particular property names - this will work one on the first level
    /// of the 
    /// </summary>
    /// <seealso cref="https://github.com/JamesNK/Newtonsoft.Json/blob/master/Src/Newtonsoft.Json/Converters/ExpandoObjectConverter.cs"/>
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
                    throw new JsonSerializationException($"Unexpected token when converting ExpandoObject using ExpandoJsonPathConverter: {reader.TokenType}");
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

                        //object val = null;
                        if (IsPrimitiveToken(reader.TokenType))
                        {
                            //val = reader.Value;
                            expandoObject[propertyName] = reader.Value;
                        }
                        else
                        {
                            var pl = PathLookups.Where(l => l.ReplacePropertyName == propertyName).FirstOrDefault();
                            if (pl?.Validate() ?? false)
                                expandoObject[pl.NewPropertyName] = GetValueFromJsonPath(reader, pl.JsonPath); ;
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

        private object GetValueFromJsonPath(JsonReader reader, string path)
        {
            object val = null;
            JObject jo = JObject.Load(reader);
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
