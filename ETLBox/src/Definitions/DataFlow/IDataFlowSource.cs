using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// Shared properties of all source components
    /// </summary>
    public interface IDataFlowSource
    {
        /// <summary>
        /// The ErrorSource is the source block used for sending errors into the linked error flow.
        /// </summary>
        ErrorSource ErrorSource { get; set; }
        /// <summary>
        /// All successor that this component is linked to.
        /// </summary>
        List<DataFlowComponent> Successors { get; }
    }

    /// <summary>
    /// Shared methods for linking of source components
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data</typeparam>
    public interface IDataFlowSource<TOutput> : IDataFlowSource
    {
        /// <summary>
        /// SourceBlock from the underlying TPL.Dataflow which is used as output buffer for the component.
        /// </summary>
        ISourceBlock<TOutput> SourceBlock { get; }
        /// <summary>
        /// Links the current block to another transformation or destination.
        /// Every component should be linked to only one component without predicates
        /// If you want to link multiple targets, either use predicates or a <see cref="ETLBox.DataFlow.Transformations.Multicast"/>
        /// </summary>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target);
        /// <summary>
        /// Links the current block to another transformation or destination with a predicate.
        /// Every component can be linked to one or more component. If you link multiple components,
        /// provide a <see cref="System.Predicate{TOutput}"/> that describe which row is send to which target.
        /// Make sure that all rows will be send to a target - use the <see cref="ETLBox.DataFlow.Connectors.VoidDestination"/>
        /// if you want to discared rows.
        /// </summary>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep);
        /// <summary>
        /// Links the current block to another transformation or destination with a predicate for rows that you want to keep
        /// and a second predicate for rows you want to discard.
        /// </summary>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);

        /// <summary>
        /// Links the current block to another transformation or destination.
        /// Every component should be linked to only one component without predicates
        /// If you want to link multiple targets, either use predicates or a <see cref="ETLBox.DataFlow.Transformations.Multicast"/>
        /// </summary>
        /// <typeparam name="TConvert">Will convert the output type of the linked component.</typeparam>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target);
        /// <summary>
        /// Links the current block to another transformation or destination with a predicate.
        /// Every component can be linked to one or more component. If you link multiple components,
        /// provide a <see cref="System.Predicate{TOutput}"/> that describe which row is send to which target.
        /// Make sure that all rows will be send to a target - use the <see cref="ETLBox.DataFlow.Connectors.VoidDestination"/>
        /// if you want to discared rows.
        /// </summary>
        /// <typeparam name="TConvert">Will convert the output type of the linked component.</typeparam>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep);
        /// <summary>
        /// Links the current block to another transformation or destination with a predicate for rows that you want to keep
        /// and a second predicate for rows you want to discard.
        /// </summary>
        /// <typeparam name="TConvert">Will convert the output type of the linked component.</typeparam>
        /// <param name="target">Transformation or destination that the block is linked to.</param>
        /// <returns>The linked component.</returns>
        IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);
        /// <summary>
        /// If an error occurs in the component, by default the component will throw an exception and stop execution.
        /// If you use the error linking, any erroneous records will catched and redirected.
        /// </summary>
        /// <param name="target">The target for erroneous rows.</param>
        /// <returns>The linked component.</returns>
        IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target);
    }

    /// <summary>
    /// Shared methods for sources that can be executed
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data</typeparam>
    public interface IDataFlowExecutableSource<TOutput> : IDataFlowSource<TOutput>
    {
        /// <summary>
        /// Starts the data flow synchronously. This method will return when all data was posted into the flow
        /// </summary>
        void Execute();

        /// <summary>
        /// Starts the data flow asynchronously. This method will return an awaitable task that completes or faults when the flow ran to completion.
        /// </summary>
        /// <returns></returns>
        Task ExecuteAsync();
    }

    /// <summary>
    /// Implemented by data flow sources that allow reading data from a stream source
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data</typeparam>
    public interface IDataFlowStreamSource<TOutput> : IDataFlowExecutableSource<TOutput>
    {
        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute).
        /// </summary>
        string Uri { get; set; }

        /// <summary>
        /// This func returns the next url that is used for reading data. It will be called until <see cref="HasNextUri"/> returns false.
        /// The incoming <see cref="StreamMetaData"/> holds information about the current progress and other meta data from the response, like unparsed
        /// json data that contains references to the next page of the response.
        /// This property can be used if you want to read multiple files or if you want to paginate through web responses.
        /// </summary>
        Func<StreamMetaData, string> GetNextUri { get; set; }

        /// <summary>
        /// This func determines if another request is started to read additional data from the next uri.
        /// <see cref="StreamMetaData"/> has information about the current progress and other meta data from the response.
        /// </summary>
        Func<StreamMetaData, bool> HasNextUri { get; set; }

        /// <summary>
        /// Specifies the resource type. By default requests are made with HttpClient.
        /// Specify ResourceType.File if you want to read from a json file.
        /// </summary>
        ResourceType ResourceType { get; set; }

        /// <summary>
        /// The System.Net.Http.HttpClient uses for the request. Use this client if you want to
        /// add or change the http request data, e.g. you can add your authorization information here.
        /// </summary>
        HttpClient HttpClient { get; set; }

        /// <summary>
        /// The System.Net.Http.HttpRequestMessage use for the request from the HttpClient. Add your request
        /// message here, e.g. your POST body.
        /// </summary>
        HttpRequestMessage HttpRequestMessage { get; set; }

    }
}
