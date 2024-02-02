using System.Net.Http;
using System.Text;
using ETLBox.Primitives;

namespace ALE.ETLBox.DataFlow
{
    public sealed class SampleHttpClient : IHttpClient
    {
        private readonly HttpClient _httpClient = new();

        public void Dispose()
        {
            _httpClient.Dispose();
        }

        public async Task<string> InvokeAsync(
            string url,
            HttpMethod method,
            IDictionary<string, string> headers,
            string body
        )
        {
            using var request = new HttpRequestMessage(method, url);
            if (method == HttpMethod.Post || method == HttpMethod.Put)
            {
                    request.Content = new StringContent(body, Encoding.UTF8, "application/json");
            }

            foreach (var header in headers)
            {
                request.Headers.Add(header.Key, header.Value);
            }

            var response = await _httpClient.SendAsync(request);

            if (response.IsSuccessStatusCode)
            {
                return await response.Content.ReadAsStringAsync();
            }
            else
            {
                throw new HttpStatusCodeException(
                    response.StatusCode,
                    await response.Content.ReadAsStringAsync()
                );
            }
        }
    }
}
