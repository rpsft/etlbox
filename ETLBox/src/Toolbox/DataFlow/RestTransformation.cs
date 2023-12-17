using System.Net.Http;
using System.Text.Json;
using ALE.ETLBox.Helper;
using ALE.ETLBox.src.Toolbox.DataFlow;
using DotLiquid;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.DataFlow
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
            TransformationFunc = source => RestMethodAsync(source).GetAwaiter().GetResult();
        }

        public RestTransformation(IHttpClient client) : this()
        {
            _httpClient = client;
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

            var method = GetMethod(RestMethodInfo.Method);
            var templateUrl = Template.Parse(RestMethodInfo.Url);
            var url = templateUrl.Render(Hash.FromDictionary(input));
            var templateBody = Template.Parse(RestMethodInfo.Body);
            var body = templateBody.Render(Hash.FromDictionary(input));

            var httpClient = _httpClient ?? new SampleHttpClient();
            var retryCount = 0;
            while (retryCount <= RestMethodInfo.RetryCount)
            {
                try
                {
                    var response = await httpClient.InvokeAsync(url, method, RestMethodInfo.Headers, body)
                        .ConfigureAwait(false);

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

                await Task.Delay(RestMethodInfo.RetryInterval * 1000)
                    .ConfigureAwait(false);
            }

            throw new InvalidOperationException();
        }

        private static HttpMethod GetMethod(string method)
        {
            return method.ToUpperInvariant() switch
            {
                "GET" => HttpMethod.Get,
                "PUT" => HttpMethod.Put,
                "POST" => HttpMethod.Post,
                "HEAD" => HttpMethod.Head,
                "DELETE" => HttpMethod.Delete,
                "OPTIONS" => HttpMethod.Options,
                "TRACE" => HttpMethod.Trace,
                _ => throw new ArgumentOutOfRangeException(nameof(method))
            };
        }
#nullable disable
    }
}
