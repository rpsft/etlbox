using System;
using JetBrains.Annotations;

namespace ALE.ETLBox.Serialization
{
    /// <summary>
    /// Optional capability that an <see cref="IDataFlow"/> implementation may provide to own and
    /// deduplicate arbitrary <see cref="IDisposable"/> resources (e.g. non-ADO.NET clients such as
    /// <c>MongoClient</c> or <c>HttpClient</c> that have no natural <see cref="ETLBox.Primitives.IConnectionManager"/>).
    /// </summary>
    /// <remarks>
    /// This is a <b>separate, optional</b> interface — intentionally NOT part of <see cref="IDataFlow"/> —
    /// so that adding resource ownership does not binary-break existing external <see cref="IDataFlow"/>
    /// implementations compiled against earlier ETLBox versions. The XML reader probes for this
    /// capability via a type check and falls back to plain instance creation (without dedup/ownership)
    /// when the data flow does not implement it.
    /// </remarks>
    [PublicAPI]
    public interface IDataFlowResourceOwner
    {
        /// <summary>
        /// Capability version of the resource-ownership contract. Consumers gate calls on a minimum
        /// version so the contract can evolve without breaking older implementations.
        /// </summary>
        int Version { get; }

        /// <summary>
        /// Gets or adds a disposable resource to the flow's ownership pool.
        /// </summary>
        /// <remarks>
        /// The data flow owns resources added via this method and disposes them when the data flow
        /// itself is disposed. Resources with identical <paramref name="key"/> values are deduplicated —
        /// subsequent calls with the same key return the existing instance without invoking
        /// <paramref name="factory"/>. Only resources whose lifetime the data flow owns should be added
        /// here; externally managed resources (e.g. resolved from a DI container) must not be.
        /// </remarks>
        /// <param name="key">
        /// Deduplication key. Resources that share a key share an instance. A typical key is the
        /// type's full name combined with a serialized representation of its configuration.
        /// </param>
        /// <param name="factory">
        /// Creates the resource when no entry for <paramref name="key"/> exists yet.
        /// </param>
        IDisposable GetOrAddResource(string key, Func<IDisposable> factory);
    }

    /// <summary>
    /// Extension methods for <see cref="IDataFlowResourceOwner"/>.
    /// </summary>
    public static class DataFlowResourceOwnerExtensions
    {
        /// <summary>
        /// Current capability version of the resource-ownership contract.
        /// </summary>
        public const int ResourceOwnerVersion = 1;

        /// <summary>
        /// Gets or adds a typed disposable resource to the flow's ownership pool.
        /// </summary>
        /// <typeparam name="T">The concrete resource type.</typeparam>
        /// <param name="owner">The data flow that owns the resource.</param>
        /// <param name="key">Deduplication key.</param>
        /// <param name="factory">Creates the resource if no entry for <paramref name="key"/> exists yet.</param>
        public static T GetOrAddResource<T>(
            this IDataFlowResourceOwner owner,
            string key,
            Func<T> factory
        )
            where T : IDisposable => (T)owner.GetOrAddResource(key, () => factory());
    }
}
