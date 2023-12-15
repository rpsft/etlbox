using System.Net;

namespace ALE.ETLBox
{
    internal class HttpStatusCodeException: Exception
    {
        public HttpStatusCodeException(HttpStatusCode statusCode, string content) : base(content)
        {
            HttpCode = statusCode;
            Content = content;
        }

        public HttpStatusCode HttpCode { get; }

        public string Content { get; }
    }
}
