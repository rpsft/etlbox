using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public interface IDataFlowLogging
    {
        /// <summary>
        /// The amount of rows the current component has already processed.
        /// </summary>
        int ProgressCount { get; set; }

        /// <summary>
        /// To avoid getting log message for every message, by default only log message are produced when 1000 rows
        /// are processed. Set this property to decrease or increase this value.
        /// </summary>
        int? LoggingThresholdRows { get; set; }
    }
}
