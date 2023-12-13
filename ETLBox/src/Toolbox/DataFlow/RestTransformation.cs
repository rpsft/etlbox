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
        private readonly JsonSerializerOptions _jsonSerializerOptions = new()
        {
            Converters = { new ExpandoObjectConverter() }
        };

        private readonly IHttpClient _httpClient;

        public ILogger Logger { get; set; }

        public RestMethodInfo RestMethodInfo { get; set; }

        public string ResultField { get; set; }

        public RestTransformation()
        {
            TransformationFunc = (ExpandoObject source) => RestMethodAsync(source).Result;
        }

        public RestTransformation(ILogger logger, IHttpClient client, RestMethodInfo restMethodInfo, string resultField) : this()
        {
            _httpClient = client;

            Logger = logger;            
            RestMethodInfo = restMethodInfo;
            ResultField = resultField;
        }

#nullable enable
        public async Task<ExpandoObject> RestMethodAsync(ExpandoObject input)
        {
            if (RestMethodInfo == null)
            {
                throw new ArgumentNullException(nameof(RestMethodInfo));
            }
            if (ResultField == null)
            {
                throw new ArgumentNullException(nameof(ResultField));
            }

            var templateUrl = Template.Parse(RestMethodInfo.Url);
            var url = templateUrl.Render(Hash.FromAnonymousObject(input));
            var templateBody = Template.Parse(RestMethodInfo.Body);
            var body = templateBody.Render(Hash.FromAnonymousObject(input));

            var httpClient = _httpClient ?? new HttpClient();
            var retryCount = 0;
            while (retryCount <= RestMethodInfo.RetryCount)
            {
                try
                {
                    var response = await httpClient.InvokeAsync(url, RestMethodInfo.Method, RestMethodInfo.Headers, body);

                    var outputValue =
                        (ExpandoObject?)
                        JsonSerializer.Deserialize(response,
                        typeof(ExpandoObject),
                        _jsonSerializerOptions
                        ) ?? throw new InvalidOperationException();

                    var res = input as IDictionary<string, object>;
                    res.Add(ResultField, outputValue);

                    if (_httpClient == null)
                    {
                        httpClient.Dispose();
                    }
                    return (ExpandoObject)res;
                }
                catch (HttpStatusCodeException ex)
                {
                    if ((int)ex.HttpCode / 100 == 5)
                    {
                        if (Logger != null)
                        {
                            //NOTE: Для HttpCode = 5XX - выполнить переотправку запроса (Retry), затем запись в лог
                            Logger.LogInformation($"Request for RestMethodInfo: \n{RestMethodInfo}\n get exception HttpCode = {ex.HttpCode}");
                        }

                        if (retryCount >= RestMethodInfo.RetryCount)
                        {
                            if (_httpClient == null)
                            {
                                httpClient.Dispose();
                            }
                            throw;
                        }
                    }
                    if ((int)ex.HttpCode / 100 == 3)
                    {
                        if (Logger != null)
                        {
                            //NOTE: 1.3.4.2.1.2.Для HttpCode = 3XX - запись в лог
                            Logger.LogInformation($"Request for RestMethodInfo: \n{RestMethodInfo}\n get exception HttpCode = {ex.HttpCode}");
                        }

                        if (_httpClient == null)
                        {
                            httpClient.Dispose();
                        }
                        throw;
                    }
                }
                retryCount++;

                await Task.Delay(RestMethodInfo.RetryInterval * 1000);
            }

            throw new InvalidOperationException();
        }
#nullable disable
    }
}
