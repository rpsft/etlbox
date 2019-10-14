using System;

namespace ALE.ETLBox
{
    /// <summary>
    /// The generic ETLBox Exception. See inner exception for more details.
    /// </summary>
    public class ETLBoxException : Exception
    {
        public ETLBoxException() : base() { }
        public ETLBoxException(string message) : base(message) { }
        public ETLBoxException(string message, Exception innerException) : base(message, innerException) { }
    }
}
