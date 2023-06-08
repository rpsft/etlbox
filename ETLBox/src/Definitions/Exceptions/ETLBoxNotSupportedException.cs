namespace ALE.ETLBox
{
    /// <summary>
    /// The generic ETLBox Exception. See inner exception for more details.
    /// </summary>
    public class ETLBoxNotSupportedException : Exception
    {
        public ETLBoxNotSupportedException() { }

        public ETLBoxNotSupportedException(string message)
            : base(message) { }

        public ETLBoxNotSupportedException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
