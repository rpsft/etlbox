#nullable enable
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace ALE.ETLBox.Common.DataFlow.Streaming;

/// <summary>
/// Persists and retrieves the streaming cursor for a tail source.
/// The payload is opaque — only the owning source interprets it.
/// </summary>
[PublicAPI]
public interface ICheckpointStore
{
    /// <summary>Returns the last saved checkpoint payload, or <c>null</c> if none exists.</summary>
    Task<string?> LoadAsync(CancellationToken ct);

    /// <summary>Persists <paramref name="payload"/> as the current checkpoint.</summary>
    Task SaveAsync(string payload, CancellationToken ct);
}
