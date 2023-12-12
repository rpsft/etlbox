using System.Net.Http;
using ALE.ETLBox.src.Definitions.Exceptions;

namespace ALE.ETLBox.src.Definitions.DataFlow
{
    internal class HttpClient : IHttpClient
    {
        private readonly System.Net.Http.HttpClient _httpClient = new System.Net.Http.HttpClient();

        public void Dispose()
        {
            _httpClient.Dispose();
        }

        public async Task<string> InvokeAsync(string url, HttpMethod method, Tuple<string, string>[] headers, string body)
        {
            using (var request = new HttpRequestMessage(method, url))
            {
                if (method == HttpMethod.Post || method == HttpMethod.Put)
                {
                    request.Content = new StringContent(body);
                }

                foreach (var header in headers)
                {
                    request.Headers.Add(header.Item1, header.Item2);
                }

                var response = await _httpClient.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    return await response.Content.ReadAsStringAsync();
                }
                else
                {
                    throw new HttpStatusCodeException(response.StatusCode, await response.Content.ReadAsStringAsync());
                }
            }
        }
    }
}
