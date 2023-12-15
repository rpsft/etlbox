using System.Net.Http;

namespace ALE.ETLBox.DataFlow
{
    public interface IHttpClient: IDisposable 
    {
        Task<string> InvokeAsync(string url, HttpMethod method, IDictionary<string,string> headers, string body);
    }
}
