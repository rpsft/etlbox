using System;
using System.Collections.Generic;
using System.Dynamic;
using ETLBox.Primitives;

namespace ALE.ETLBox.Serialization
{
    /// <summary>
    /// Data flow graph that can be executed.
    /// </summary>
    /// <remarks>
    /// This abstraction can be used when the graph is not created programmatically but rather
    /// stored in some kind of configuration file (e.g., XML).
    ///
    /// Calling convention is as follows:
    /// - Create instance of <see cref="IDataFlow"/> implementation
    /// - Initialize from XML using <see cref="System.Xml.Serialization.XmlSerializer"/>
    /// - Call Source.Execute() to start the dataflow
    /// - Wait for all destinations to complete using <see cref="IDataFlowDestination{T}.Completion"/>
    /// - Wait for all error destinations to complete using <see cref="IDataFlowDestination{T}.Completion"/>
    /// </remarks>
    public interface IDataFlow : IDisposable
    {
        /// <summary>
        /// Get or add a connection manager to the pool.
        /// </summary>
        /// <remarks>
        /// Data flow implementation owns resources for the connection manager and will dispose of it when data flow is completed.
        /// Use this method to get or add a connection manager to the pool so that other components can use it.
        /// </remarks>
        /// <param name="connectionManagerType">Connection manager type. Must implement IConnectionManager</param>
        /// <param name="key">Unique identifier of a connection manager in the pool, like connection string</param>
        /// <param name="factory">Connection manager factory</param>
        IConnectionManager GetOrAddConnectionManager(
            Type connectionManagerType,
            string? key,
            Func<Type, string?, IConnectionManager> factory
        );

        /// <summary>
        /// Gets or adds a disposable resource to the flow's ownership pool.
        /// </summary>
        /// <remarks>
        /// The data flow owns resources added via this method and disposes them when the data flow
        /// itself is disposed. Resources with identical <paramref name="key"/> values are deduplicated —
        /// subsequent calls with the same key return the existing instance without invoking
        /// <paramref name="factory"/>. Use this for non-ADO.NET clients (e.g. <c>MongoClient</c>,
        /// <c>HttpClient</c>) that have no natural <see cref="IConnectionManager"/> equivalent.
        /// </remarks>
        /// <param name="key">
        /// Deduplication key. Resources that share a key share an instance. A typical key is the
        /// type's full name combined with a serialized representation of its configuration.
        /// </param>
        /// <param name="factory">
        /// Creates the resource when no entry for <paramref name="key"/> exists yet.
        /// </param>
        IDisposable GetOrAddResource(string key, Func<IDisposable> factory);

        /// <summary>
        /// Single source of data transformations in the data flow.
        /// </summary>
        IDataFlowSource<ExpandoObject> Source { get; set; }

        /// <summary>
        /// All data destinations and data transformations in the flow.
        /// </summary>
        IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; }

        /// <summary>
        /// All error destinations of data flow.
        /// </summary>
        IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; }
    }

    /// <summary>
    /// Extension methods for <see cref="IDataFlow"/>.
    /// </summary>
    public static class DataFlowExtensions
    {
        /// <summary>
        /// Gets or adds a typed disposable resource to the flow's ownership pool.
        /// </summary>
        /// <typeparam name="T">The concrete resource type.</typeparam>
        /// <param name="dataFlow">The data flow that owns the resource.</param>
        /// <param name="key">Deduplication key.</param>
        /// <param name="factory">Creates the resource if no entry for <paramref name="key"/> exists yet.</param>
        public static T GetOrAddResource<T>(this IDataFlow dataFlow, string key, Func<T> factory)
            where T : IDisposable => (T)dataFlow.GetOrAddResource(key, () => factory());
    }
}
