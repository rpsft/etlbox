using System.Net.Http;

namespace ALE.ETLBox.src.Definitions.DataFlow
{
    internal class HttpClient : IHttpClient
    {
        readonly System.Net.Http.HttpClient _httpClient = new System.Net.Http.HttpClient();

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

                return await response.Content.ReadAsStringAsync();
            }
        }
    }
}
