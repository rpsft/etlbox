namespace ALE.ETLBox.src.Definitions.DataFlow.Type
{
    internal sealed class RestMethodInfo
    {
        /// <summary>
        /// шаблон url (формат Liquid)
        /// </summary>
        internal string Url { get; set; }

        /// <summary>
        /// массив заголовков
        /// </summary>
        internal string[] Headers { get; set; }

        /// <summary>
        /// { GET, POST, PUT, DELETE }
        /// </summary>
        internal string Method { get; set; }

        /// <summary>
        /// шаблон тела запроса(формат Liquid)
        /// </summary>
        internal string Body { get; set; }

        /// <summary>
        /// количество повторений запроса
        /// </summary>
        internal int RetryCount { get; set; }

        /// <summary>
        /// пауза между попытками(сек.) повторения запроса
        /// </summary>
        internal int RetryInterval { get; set; }
    }
}
