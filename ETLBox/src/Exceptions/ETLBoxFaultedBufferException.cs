using System;

namespace ETLBox.Exceptions
{
    /// <summary>
    /// The generic ETLBox Exception. See inner exception for more details.
    /// </summary>
    public class ETLBoxFaultedBufferException : Exception
    {
        public ETLBoxFaultedBufferException() : base() { }
        public ETLBoxFaultedBufferException(string message) : base(message) { }
        public ETLBoxFaultedBufferException(string message, Exception innerException) : base(message, innerException) { }
    }
}
