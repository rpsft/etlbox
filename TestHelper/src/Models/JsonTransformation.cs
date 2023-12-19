using System;
using System.Collections.Generic;
using System.Dynamic;
using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using Newtonsoft.Json.Linq;

namespace TestHelper.Models
{
    [PublicAPI]
    public class JsonTransformation : RowTransformation<ExpandoObject>
    {
        public JsonTransformation()
        {
            TransformationFunc = source =>
            {
                var res = new ExpandoObject() as IDictionary<string, object>;
                if (Mappings != null)
                {
                    foreach (var mapping in Mappings)
                    {
                        res.Add(mapping.Destination, GetValue(source, mapping));
                    }
                }

                if (Mappings2 != null)
                {
                    for (var i = 0; i < Mappings2.Length; i++)
                    {
                        res.Add($"Mappings2#{i}", Mappings2[i]);
                    }
                }

                if (StringMappings != null)
                {
                    foreach (var mapping in StringMappings)
                    {
                        res.Add(mapping.Key, mapping.Value);
                    }
                }

                if (DateTimeMappings != null)
                {
                    foreach (var mapping in DateTimeMappings)
                    {
                        res.Add(mapping.Key, mapping.Value);
                    }
                }

                if (JsonMappings != null)
                {
                    foreach (var mapping in JsonMappings)
                    {
                        res.Add(mapping.Key, GetValue(source, mapping.Value));
                    }
                }

                return (ExpandoObject)res;
            };
        }

        public JsonMapping[] Mappings { get; set; }

        public string[] Mappings2 { get; set; }

        public Dictionary<string, string> StringMappings { get; set; }
        public Dictionary<string, DateTime> DateTimeMappings { get; set; }
        public Dictionary<string, JsonMapping> JsonMappings { get; set; }

        private static string GetValue(ExpandoObject source, JsonMapping mapping)
        {
            var values = source as IDictionary<string, object>;
            // Parse the JSON string
            var jsonObj = JObject.Parse(values[mapping.Source.Name].ToString());

            // Use JSONPath to retrieve the value
            JToken value = jsonObj.SelectToken(mapping.Source.Path);

            // Convert the value to string
            return value?.ToString();
        }
    }
}
