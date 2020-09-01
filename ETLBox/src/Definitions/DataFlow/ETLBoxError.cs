using System;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// The generic ETLBox Exception. Contains serialized information about the error.
    /// </summary>
    public class ETLBoxError
    {
        /// <summary>
        /// The exceptions message as string
        /// </summary>
        public string ErrorText { get; set; }
        /// <summary>
        /// The point in time when the error occured
        /// </summary>
        public DateTime ReportTime { get; set; }
        /// <summary>
        /// The exception type as string
        /// </summary>
        public string ExceptionType { get; set; }
        /// <summary>
        /// The erroneous records serialized as json
        /// </summary>
        public string RecordAsJson { get; set; }
    }
}
