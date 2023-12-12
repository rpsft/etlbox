using System.Net.Http;

namespace ALE.ETLBox.src.Definitions.DataFlow
{
    public interface IHttpClient: IDisposable 
    {
        Task<string> InvokeAsync(string url, HttpMethod method, Tuple<string, string>[] headers, string body);
    }
}
