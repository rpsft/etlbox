#nullable enable
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using ALE.ETLBox;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using DotLiquid;
using ETLBox.Primitives;
using ETLBox.Rest.Models;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;

namespace ETLBox.Rest
{
    [PublicAPI]
    public class RestTransformation : RowTransformation<ExpandoObject>
    {
        private static readonly Func<IHttpClient> s_defaultHttpClientFactory = () => new SampleHttpClient();

        private readonly JsonSerializerOptions _jsonSerializerOptions = new JsonSerializerOptions()
        {
            Converters = { new ExpandoObjectConverter() }
        };

        private readonly Func<IHttpClient> _httpClientFactory;

        public RestMethodInfo RestMethodInfo { get; set; } = null!;

        public string ResultField { get; set; } = null!;

        public string ExceptionField { get; set; } = null!;

        public bool FailOnError { get; set; } = true;

        public RestTransformation()
        {
            _httpClientFactory = s_defaultHttpClientFactory;

            TransformationFunc = source => RestMethodAsync(source).GetAwaiter().GetResult();
        }

        public RestTransformation(Func<IHttpClient>? httpClientFactory) : this()
        {
            _httpClientFactory = () => new SampleHttpClient();
            _httpClientFactory = httpClientFactory ?? s_defaultHttpClientFactory;
        }

        public async Task<ExpandoObject> RestMethodAsync(ExpandoObject input)
        {
            IHttpClient? httpClient = null;
            try
            {
                httpClient = _httpClientFactory();
                var result = await RestMethodInternalAsync(input, httpClient)
                    .ConfigureAwait(false);
                LogProgress();
                return result;
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer)
                    throw;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(input));
            }
            finally
            {
                httpClient?.Dispose();
            }

            return new ExpandoObject();
        }

        private async Task<ExpandoObject> RestMethodInternalAsync(ExpandoObject input, IHttpClient httpClient)
        {
            if (input is null)
            {
                throw new ArgumentNullException(nameof(input));
            }
            if (RestMethodInfo is null)
            {
                throw new InvalidOperationException($"Property '{nameof(RestMethodInfo)}' not defined");
            }
            if (ResultField is null)
            {
                throw new InvalidOperationException($"Property '{nameof(ResultField)}' not defined");
            }

            var method = GetMethod(RestMethodInfo?.Method!);
            var templateUrl = Template.Parse(RestMethodInfo?.Url!);
            var url = templateUrl.Render(Hash.FromDictionary(input));
            Logger.LogTrace($"Headers: {string.Join(", ", RestMethodInfo?.Headers.Select(k => $"{k.Key}: {k.Value}"))}");
            Logger.LogTrace($"Url: {url}");
            var templateBody = Template.Parse(RestMethodInfo?.Body);
            var body = templateBody.Render(Hash.FromDictionary(input));
            Logger.LogTrace($"Body: {body}");

            var retryCount = 0;
            while (retryCount <= RestMethodInfo?.RetryCount)
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
                    LogProgress();
                    return (ExpandoObject)res;
                }
                catch (HttpStatusCodeException ex)
                {
                    if (HandleHttpStatusCodeException(retryCount, ex))
                    {
                        return HandleError(input, ex);
                    }
                }
                catch (Exception ex)
                {
                    return HandleError(input, ex);
                }
                retryCount++;

                await Task.Delay(RestMethodInfo.RetryInterval * 1000)
                    .ConfigureAwait(false);
            }

            throw new InvalidOperationException();
        }

        private bool HandleHttpStatusCodeException(int retryCount, HttpStatusCodeException ex)
        {
            if ((int)ex.HttpCode / 100 == 5)
            {
                Logger.LogInformation($"Request for RestMethodInfo: \n{RestMethodInfo}\n get exception HttpCode = {ex.HttpCode}");

                if (retryCount >= RestMethodInfo.RetryCount)
                {
                    return true;
                }
            }
            if ((int)ex.HttpCode / 100 == 3)
            {
                Logger.LogInformation($"Request for RestMethodInfo: \n{RestMethodInfo}\n get exception HttpCode = {ex.HttpCode}");

                return true;
            }
            return false;
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

        private ExpandoObject HandleError(ExpandoObject input, Exception ex)
        {
            if (FailOnError)
            {
                throw ex;
            }
            Logger.LogError(ex, ex.Message);
            var res = input as IDictionary<string, object>;
            var exObj = new ExpandoObject() as IDictionary<string, object?>;
            exObj.Add(ExceptionField, $"Exception: {ex.Message}");
            res.Add(ResultField, (ExpandoObject)exObj);
            return (ExpandoObject)res;
        }
    }
}
