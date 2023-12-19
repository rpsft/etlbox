using System.Net;
using System.Runtime.Serialization;

namespace ALE.ETLBox
{
    [Serializable]
    public class HttpStatusCodeException : Exception
    {
        protected HttpStatusCodeException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public HttpStatusCodeException() 
        { 
        }

        public HttpStatusCodeException(HttpStatusCode statusCode, string content) : base(content)
        {
            HttpCode = statusCode;
            Content = content;
        }

        public HttpStatusCode HttpCode { get; }

        public string Content { get; }
    }
}
