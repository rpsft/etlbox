using ALE.ETLBox.DataFlow.Mappings;
using Newtonsoft.Json.Linq;

namespace ALE.ETLBox.DataFlow
{
    [PublicAPI]
    public class JsonTransformation : RowTransformation<ExpandoObject>
    {
        public JsonTransformation()
        {
            TransformationFunc = source =>
            {
                var res = new ExpandoObject() as IDictionary<string, object>;
                foreach (var mapping in Mappings)
                {
                    res.Add(mapping.Destination, GetValue(source, mapping));
                }
                return (ExpandoObject)res;
            };
        }

        public JsonMapping[] Mappings { get; set; }

        private static string GetValue(ExpandoObject source, JsonMapping mapping)
        {
            var values = source as IDictionary<string, object>;
            // Parse the JSON string
            JObject jsonObj = JObject.Parse(values[mapping.Source.Name].ToString());

            // Use JSONPath to retrieve the value
            JToken value = jsonObj.SelectToken(mapping.Source.Path);

            // Convert the value to string
            return value?.ToString();
        }
    }
}
