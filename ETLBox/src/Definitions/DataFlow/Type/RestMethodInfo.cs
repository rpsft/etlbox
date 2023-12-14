using System.Net.Http;

namespace ALE.ETLBox.DataFlow
{
    public sealed class RestMethodInfo
    {
        /// <summary>
        /// шаблон url (формат Liquid)
        /// </summary>
        public string Url { get; set; }

        /// <summary>
        /// массив заголовков
        /// </summary>
        public Tuple<string, string>[] Headers { get; set; }

        /// <summary>
        /// { GET, POST, PUT, DELETE }
        /// </summary>
        public HttpMethod Method { get; set; }

        /// <summary>
        /// шаблон тела запроса(формат Liquid)
        /// </summary>
        public string Body { get; set; }

        /// <summary>
        /// количество повторений запроса
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// пауза между попытками(сек.) повторения запроса
        /// </summary>
        public int RetryInterval { get; set; }
    }
}
