using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace ETLBox.Primitives
{
    public interface IHttpClient: IDisposable 
    {
        Task<string> InvokeAsync(string url, HttpMethod method, IDictionary<string,string> headers, string body);
    }
}
