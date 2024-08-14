using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
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
    /// <summary>
    /// Represents a transformation that performs REST API calls.
    /// </summary>
    [PublicAPI]
    public class RestTransformation : RowTransformation<ExpandoObject>
    {
        private static readonly Func<IHttpClient> s_defaultHttpClientFactory = () =>
            new SampleHttpClient();

        private readonly JsonSerializerOptions _jsonSerializerOptions =
            new() { Converters = { new ExpandoObjectConverter() } };

        private readonly Func<IHttpClient> _httpClientFactory;

        /// <summary>
        /// Gets or sets the information about the REST method to be invoked.
        /// </summary>
        public RestMethodInfo RestMethodInfo { get; set; } = null!;

        /// <summary>
        /// Gets or sets the field name where the HTTP code of the REST call will be stored.
        /// </summary>
        public string HttpCodeField { get; set; } = null!;

        /// <summary>
        /// Gets or sets the field name where the result of the REST call will be stored.
        /// </summary>
        public string ResultField { get; set; } = null!;

        /// <summary>
        /// Gets or sets the field name where the raw response string of the REST call will be stored.
        /// </summary>
        public string RawResponseField { get; set; } = null!;

        /// <summary>
        /// Gets or sets the field name where any exception message will be stored.
        /// </summary>
        public string ExceptionField { get; set; } = null!;

        /// <summary>
        /// Indicates whether the transformation should fail on error or continue with optional error logging.
        /// </summary>
        public bool FailOnError { get; set; } = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="RestTransformation"/> class with default HTTP client factory.
        /// </summary>
        public RestTransformation()
        {
            _httpClientFactory = s_defaultHttpClientFactory;

            TransformationFunc = source => RestMethodAsync(source).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RestTransformation"/> class with a specified HTTP client factory.
        /// </summary>
        /// <param name="httpClientFactory">The factory method to create an instance of IHttpClient.</param>
        public RestTransformation(Func<IHttpClient>? httpClientFactory)
            : this()
        {
            _httpClientFactory = () => new SampleHttpClient();
            _httpClientFactory = httpClientFactory ?? s_defaultHttpClientFactory;
        }

        /// <summary>
        /// Transforms the input data by invoking a REST method.
        /// </summary>
        [SuppressMessage(
            "Critical Bug",
            "S4275: Getters and setters should access the expected fields",
            Justification = "Just sealing base class property"
        )]
        public sealed override Func<ExpandoObject, ExpandoObject> TransformationFunc
        {
            get => base.TransformationFunc;
            set => base.TransformationFunc = value;
        }

        /// <summary>
        /// Asynchronously invokes the REST method using the provided input.
        /// </summary>
        /// <param name="input">The input data for the REST call.</param>
        /// <returns>The result of the REST call as an ExpandoObject.</returns>

        public async Task<ExpandoObject> RestMethodAsync(ExpandoObject input)
        {
            IHttpClient? httpClient = null;
            try
            {
                httpClient = _httpClientFactory();
                var result = await RestMethodInternalAsync(input, httpClient).ConfigureAwait(false);
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

        private async Task<ExpandoObject> RestMethodInternalAsync(
            ExpandoObject input,
            IHttpClient httpClient
        )
        {
            if (input is null)
            {
                throw new ArgumentNullException(nameof(input));
            }
            if (RestMethodInfo is null)
            {
                throw new InvalidOperationException(
                    $"Property '{nameof(RestMethodInfo)}' not defined"
                );
            }
            if (ResultField is null)
            {
                throw new InvalidOperationException(
                    $"Property '{nameof(ResultField)}' not defined"
                );
            }
            if (HttpCodeField is null)
            {
                throw new InvalidOperationException(
                    $"Property '{nameof(HttpCodeField)}' not defined"
                );
            }

            var method = GetMethod(RestMethodInfo.Method!);
            var templateUrl = Template.Parse(RestMethodInfo.Url!);
            var url = templateUrl.Render(Hash.FromDictionary(input));
            Logger.LogTrace(
                "Headers: {Headers}",
                string.Join(", ", RestMethodInfo.Headers.Select(k => $"{k.Key}: {k.Value}"))
            );
            Logger.LogTrace("Url: {Url}", url);
            var templateBody = Template.Parse(RestMethodInfo.Body);
            var body = templateBody.Render(Hash.FromDictionary(input));
            Logger.LogTrace("Body: {Body}", body);

            var retryCount = 0;
            while (retryCount < RestMethodInfo.RetryCount)
            {
                try
                {
                    var response = await httpClient
                        .InvokeAsync(url, method, RestMethodInfo.Headers, body)
                        .ConfigureAwait(false);
                    var outputValue = GetResponseObject(response);

                    var res = input as IDictionary<string, object?>;
                    res[HttpCodeField] = HttpStatusCode.OK;
                    res[ResultField] = outputValue;
                    if (!string.IsNullOrEmpty(RawResponseField))
                    { 
                        res[RawResponseField] = response;
                    }
                    LogProgress();
                    return (ExpandoObject)res;
                }
                catch (HttpStatusCodeException ex)
                {
                    if (TryHandleHttpStatusCodeException(retryCount, ex))
                    {
                        return HandleError(input, ex);
                    }
                }
                catch (Exception ex)
                {
                    return HandleError(input, ex);
                }
                retryCount++;

                await Task.Delay(TimeSpan.FromSeconds(RestMethodInfo.RetryInterval))
                    .ConfigureAwait(false);
            }

            if (FailOnError)
            {
                throw new InvalidOperationException(
                    $"Could not get successful result from REST call after {retryCount - 1} retries"
                );
            }
            Logger.LogError(
                "Could not get successful result from REST call after {RetryCount} retries",
                retryCount - 1
            );
            return input;
        }

        private bool TryHandleHttpStatusCodeException(int retryCount, HttpStatusCodeException ex)
        {
            switch ((int)ex.HttpCode)
            {
                case >= 500
                and <= 599:
                {
                    Logger.LogError(
                        "Request for RestMethodInfo: {RestMethodInfo} get exception HttpCode = {ExHttpCode}, retry count is {RetryCount} out of {MaxRetryCount}",
                        RestMethodInfo.Url,
                        ex.HttpCode,
                        retryCount,
                        RestMethodInfo.RetryCount
                    );
                    return retryCount >= RestMethodInfo.RetryCount - 1;
                }
                case >= 400:
                    Logger.LogError(
                        "Request for RestMethodInfo: {RestMethodInfo} get exception HttpCode = {ExHttpCode}",
                        RestMethodInfo.Url,
                        ex.HttpCode
                    );
                    return true;
                default:
                    // We can get here if AllowAutoRedirect is set to false and the server returns a redirect status code
                    // consider it as an error
                    Logger.LogError(
                        "Request for RestMethodInfo: {RestMethodInfo} get exception HttpCode = {ExHttpCode}",
                        RestMethodInfo.Url,
                        ex.HttpCode
                    );
                    return true;
            }
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
            Logger.LogError(ex, "Failed REST transformation: '{Message}'", ex.Message);
            if (FailOnError)
            {
                throw ex;
            }
            var res = input as IDictionary<string, object?>;
            res[ExceptionField] = ex;
            if (ex is HttpStatusCodeException httpStatusCodeException)
            {
                res[ResultField] = new ExpandoObject();
                res[HttpCodeField] = httpStatusCodeException.HttpCode;
                if (!string.IsNullOrEmpty(RawResponseField))
                {
                    res[RawResponseField] = httpStatusCodeException.Content;
                }
            }
            return (ExpandoObject)res;
        }

        private ExpandoObject? GetResponseObject(string response)
        {
            try
            {
                var doc = JsonDocument.Parse(response);
                return doc.Deserialize<ExpandoObject?>(_jsonSerializerOptions);
            }
            catch
            {
                return null;
            }
        }
    }
}
