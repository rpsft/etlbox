using System.Text.Json;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Helper.JsonConverter;
using DotLiquid;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.src.Toolbox.DataFlow
{
    [PublicAPI]
    public class RestTransformation : RowTransformation<ExpandoObject>
    {
        private readonly ILogger _logger;

        private readonly IHttpClient _client;

        private readonly JsonSerializerOptions _jsonSerializerOptions = new()
        {
            Converters = { new ExpandoObjectConverter() }
        };

        public RestTransformation(ILogger logger, IHttpClient client, RestMethodInfo restMethodInfo, string resultField)
        {
            _logger = logger;
            _client = client;
            TransformationFunc = (ExpandoObject source) => RestMethodAsync(restMethodInfo, source, resultField).Result;
        }

#nullable enable
        public async Task<ExpandoObject> RestMethodAsync(RestMethodInfo restMethodInfo, ExpandoObject input, string resultField)
        {
            var templateUrl = Template.Parse(restMethodInfo.Url);
            var url = templateUrl.Render(Hash.FromAnonymousObject(input));
            var templateBody = Template.Parse(restMethodInfo.Body);
            var body = templateBody.Render(Hash.FromAnonymousObject(input));

            var retryCount = 0;
            while (retryCount <= restMethodInfo.RetryCount)
            {
                try
                {
                    var response = await _client.InvokeAsync(url, restMethodInfo.Method, restMethodInfo.Headers, body);

                    var outputValue =
                        (ExpandoObject?)
                        JsonSerializer.Deserialize(response,
                        typeof(ExpandoObject),
                        _jsonSerializerOptions
                        ) ?? throw new InvalidOperationException();

                    var res = input as IDictionary<string, object>;
                    res.Add(resultField, outputValue);

                    return (ExpandoObject)res;
                }
                catch (HttpStatusCodeException ex)
                {
                    if ((int)ex.HttpCode / 100 == 5)
                    {
                        //NOTE: Для HttpCode = 5XX - выполнить переотправку запроса (Retry), затем запись в лог
                        _logger?.LogInformation($"Request for RestMethodInfo: \n{restMethodInfo}\n get exception HttpCode = {ex.HttpCode}");

                        if (retryCount >= restMethodInfo.RetryCount)
                        {
                            throw;
                        }
                    }
                    if ((int)ex.HttpCode / 100 == 3)
                    {
                        //NOTE: 1.3.4.2.1.2.Для HttpCode = 3XX - запись в лог
                        _logger?.LogInformation($"Request for RestMethodInfo: \n{restMethodInfo}\n get exception HttpCode = {ex.HttpCode}");

                        throw;
                    }
                }
                retryCount++;

                await Task.Delay(restMethodInfo.RetryInterval * 1000);
            }

            throw new InvalidOperationException();
        }
#nullable disable
    }
}
