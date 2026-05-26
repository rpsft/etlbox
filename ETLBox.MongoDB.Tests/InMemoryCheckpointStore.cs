using ALE.ETLBox.Common.DataFlow.Streaming;

namespace ETLBox.MongoDB.Tests;

internal sealed class InMemoryCheckpointStore : ICheckpointStore
{
    private string? _payload;

    public Task<string?> LoadAsync(CancellationToken ct) => Task.FromResult(_payload);

    public Task SaveAsync(string payload, CancellationToken ct)
    {
        _payload = payload;
        return Task.CompletedTask;
    }
}
