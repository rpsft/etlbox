using System;

namespace ETLBox.Primitives
{
    /// <summary>
    /// The generic ETLBox Exception. See the inner exception for more details.
    /// </summary>
    public class ETLBoxError
    {
        public string ErrorText { get; set; }
        public DateTime ReportTime { get; set; }
        public Exception Exception { get; set; }
        public string RecordAsJson { get; set; }
    }
}
