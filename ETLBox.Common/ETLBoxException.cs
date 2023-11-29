using System;
using System.Runtime.Serialization;

namespace ALE.ETLBox.Common
{
    /// <summary>
    /// The generic ETLBox Exception. See inner exception for more details.
    /// </summary>
    [Serializable]
    public sealed class ETLBoxException : Exception
    {
        public ETLBoxException() { }

        public ETLBoxException(string message)
            : base(message) { }

        public ETLBoxException(string message, Exception innerException)
            : base(message, innerException) { }

        private ETLBoxException(SerializationInfo info, StreamingContext context)
            : base(info, context) { }
    }
}
