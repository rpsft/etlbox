using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public interface IDataFlowComponent
    {
        /// <summary>
        /// The completion task of the component. A component is completed when all predecessors (if any) are
        /// completed and the current component has completed its buffer.
        /// </summary>
        Task Completion { get;}

        /// <summary>
        /// When a component has completed and processed all rows, the OnCompletion action is executed.
        /// </summary>
        Action OnCompletion { get; set; }

        /// <summary>
        /// If a component encountered an exception or entered a fault state because another component
        /// in the data flow faulted, the thrown exception will be stored in this property.
        /// </summary>
        Exception Exception { get; }
        /// <summary>
        /// Each component can have one or more buffers to improve throughput and allow faster processing of data.
        /// Set this value to restrict the number of rows that can be stored in the buffer.
        /// The default value is -1 (unlimited)
        /// </summary>
        int MaxBufferSize { get; set; }
    }
}
