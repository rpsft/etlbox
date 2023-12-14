using System.Net;

namespace ALE.ETLBox
{
    internal class HttpStatusCodeException(HttpStatusCode statusCode, string content) : Exception(content)
    {
        public HttpStatusCode HttpCode { get; } = statusCode;

        public string Content { get; } = content;
    }
}
