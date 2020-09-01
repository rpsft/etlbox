using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// This class holds meta data about the current progress of the source.
    /// </summary>
    public class StreamMetaData
    {
        /// <summary>
        /// Number of currently processed items
        /// </summary>
        public int ProgressCount { get; set; }
        /// <summary>
        /// Unparsed meta data from the response. E.g. this could be unparsed json that holds the links to the next page of the response.
        /// </summary>
        public string UnparsedData { get; set; }
    }
}
