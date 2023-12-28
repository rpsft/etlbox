using System.Collections.Generic;
using System.Dynamic;
using ALE.ETLBox.Common.DataFlow;
using JetBrains.Annotations;
using Newtonsoft.Json.Linq;

namespace ETLBox.Json
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
                    foreach (var key in Mappings.Keys)
                    {
                        res.Add(key, GetValue(source, Mappings[key]));
                    }
                }

                return (ExpandoObject)res;
            };
        }

        public Dictionary<string, JsonMapping> Mappings { get; set; } = new Dictionary<string, JsonMapping>();

        private static object GetValue(ExpandoObject source, JsonMapping mapping)
        {
            var values = source as IDictionary<string, object>;

            // If no path is specified, use the whole field
            if (mapping.Path == null)
                return values[mapping.Name!];

             // Parse the JSON string
            var jsonObj = JObject.Parse(values[mapping.Name].ToString());

            // Use JSONPath to retrieve the value
            JToken value = jsonObj.SelectToken(mapping.Path)!;

            // Convert the value to string
            return value?.ToString()!;
        }
    }
}
