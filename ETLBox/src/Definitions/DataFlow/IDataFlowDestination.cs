using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface IDataFlowDestination
    {
        /// <summary>
        /// All predecessor that are linked to this component.
        /// </summary>
        List<DataFlowComponent> Predecessors { get; }
    }

    public interface IDataFlowDestination<TInput> : IDataFlowDestination
    {
        /// <summary>
        /// TargetBlock from the underlying TPL.Dataflow which is used as output buffer for the component.
        /// </summary>
        ITargetBlock<TInput> TargetBlock { get; }
    }

    public interface IDataFlowBatchDestination<TInput> : IDataFlowDestination<TInput>
    {
        /// <summary>
        /// Batch size that is used when inserted data in batches
        /// </summary>
        int BatchSize { get; set; }
    }

    /// <summary>
    /// Implemented by data flow destinations that allow writing data in a stream
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data</typeparam>
    public interface IDataFlowStreamDestination<TInput> : IDataFlowDestination<TInput>
    {
        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        string Uri { get; set; }

        /// <summary>
        /// Specifies the resourc type. ResourceType.
        /// Specify ResourceType.File if you want to write into a file.
        /// </summary>
        ResourceType ResourceType { get; set; }

        /// <summary>
        /// The System.Net.HttpClient used to connect with the destination (only needed when the <see cref="ResourceType"/> is Http.
        /// </summary>
        HttpClient HttpClient { get; set; }
    }
}
