using System.Collections.Generic;
using System.Dynamic;
using ETLBox.Primitives;

namespace ALE.ETLBox.Serialization
{
    /// <summary>
    /// Dataflow graph that can be executed.
    /// </summary>
    /// <remarks>
    /// This abstraction can be used when graph is not created programmatically, but rather
    /// stored in some kind of configuration file (e.g. XML).
    ///
    /// Calling convention is as follows:
    /// - Create instance of <see cref="IDataFlow"/> implementation
    /// - Initialize from XML using <see cref="IDataFlowSerializationExtensions.ReadFromXml"/>
    /// - Call Source.Execute() to start the dataflow
    /// - Wait for all destinations to complete using <see cref="IDataFlowDestination{T}.Completion"/>
    /// - Wait for all error destinations to complete using <see cref="IDataFlowDestination{T}.Completion"/>
    /// </remarks>
    public interface IDataFlow
    {
        IDataFlowSource<ExpandoObject> Source { get; set; }

        IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; }

        IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; }
    }
}
