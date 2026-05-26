using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.Common.DataFlow.Streaming;
using ETLBox.Primitives;
using JetBrains.Annotations;

namespace ALE.ETLBox.DataFlow;

/// <summary>
/// Tail-reads a PostgreSQL table using xmin-frontier polling to avoid race conditions
/// with concurrent in-flight transactions.
/// </summary>
/// <remarks>
/// Uses <c>pg_snapshot_xmin(pg_current_snapshot())</c> as a read-safe frontier so that
/// rows inserted by transactions that committed after the snapshot boundary are never missed.
/// Set <see cref="ALE.ETLBox.Common.ControlFlow.GenericTask.ConnectionManager"/> before calling <see cref="Execute"/>.
/// </remarks>
[PublicAPI]
public class PostgresXminTailSource<TOutput> : DataFlowSource<TOutput>
{
    /// <summary>Table to poll. Must expose the system column <c>xmin</c>.</summary>
    public string TableName { get; set; } = null!;

    /// <summary>Schema that contains <see cref="TableName"/>. Defaults to <c>public</c>.</summary>
    public string Schema { get; set; } = "public";

    /// <summary>
    /// Columns used for ordering and tuple-cursor pagination.
    /// Must be monotone and uniquely identify the row's position in the stream.
    /// </summary>
    public string[] OrderByColumns { get; set; } = Array.Empty<string>();

    /// <summary>Optional extra WHERE predicate appended with AND.</summary>
    public string? AdditionalWhere { get; set; }

    /// <summary>Rows per polling round. Defaults to 500.</summary>
    public int BatchSize { get; set; } = 500;

    /// <summary>Pause between polling rounds when no rows are found. Defaults to 1 second.</summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Stores and retrieves the streaming cursor across restarts.
    /// If <c>null</c>, the source always starts from the beginning of the table.
    /// </summary>
    public ICheckpointStore? CheckpointStore { get; set; }

    /// <summary>Maps a data record row to the output type. Required.</summary>
    public Func<IDataRecord, TOutput> RowMapper { get; set; } = null!;

    /// <inheritdoc/>
    public override void Execute(CancellationToken cancellationToken)
    {
        LogStart();
        try
        {
            RunPollingLoop(cancellationToken);
        }
        finally
        {
            Buffer.Complete();
            LogFinish();
        }
        cancellationToken.ThrowIfCancellationRequested();
    }

    private void RunPollingLoop(CancellationToken ct)
    {
        var checkpoint = LoadCheckpoint(ct);

        while (!ct.IsCancellationRequested)
        {
            var frontier = GetFrontier();
            var rowsRead = 0;
            object?[]? lastCursorValues = null;

            using var reader = ExecuteQuery(frontier, checkpoint);
            while (reader.Read())
            {
                ct.ThrowIfCancellationRequested();
                var output = RowMapper(reader);
                // Propagate the source's cancellation token into SendAsync so that
                // backpressure from a bounded downstream buffer doesn't trap the
                // polling loop after Cancel() — see RSSL-11703 regression test
                // Execute_CancellationDuringBlockedSendAsync_ReturnsPromptly.
                Buffer.SendAsync(output, ct).GetAwaiter().GetResult();
                lastCursorValues = ReadCursorValues(reader);
                rowsRead++;
                LogProgress();
            }

            if (rowsRead > 0 && lastCursorValues != null)
            {
                checkpoint = new CheckpointPayload(lastCursorValues);
                SaveCheckpoint(checkpoint, ct);
            }
            else
            {
                Task.Delay(PollingInterval, ct).Wait(ct);
            }
        }
    }

    private long GetFrontier()
    {
        ConnectionManager.Open();
        try
        {
            var result = ConnectionManager.ExecuteScalar(
                "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint"
            );
            return Convert.ToInt64(result);
        }
        finally
        {
            ConnectionManager.CloseIfAllowed();
        }
    }

    private IDataReader ExecuteQuery(long frontier, CheckpointPayload? checkpoint)
    {
        var sql = BuildQuery(frontier, checkpoint, out var parameters);
        ConnectionManager.Open();
        return ConnectionManager.ExecuteReader(sql, parameters);
    }

    private string BuildQuery(
        long frontier,
        CheckpointPayload? checkpoint,
        out List<IQueryParameter> parameters
    )
    {
        parameters = new List<IQueryParameter>();
        var query = new StringBuilder();

        query.Append("SELECT *, xmin::text::bigint AS _xmin_val FROM ");
        query.Append(QuotedTableRef());
        query.Append(" WHERE xmin::text::bigint < @_frontier");
        parameters.Add(new QueryParameter("_frontier", frontier));

        if (checkpoint != null && OrderByColumns.Length > 0)
        {
            AppendTupleCursor(query, parameters, checkpoint.Values);
        }

        if (!string.IsNullOrWhiteSpace(AdditionalWhere))
        {
            query.Append(" AND (");
            query.Append(AdditionalWhere);
            query.Append(")");
        }

        if (OrderByColumns.Length > 0)
        {
            query.Append(" ORDER BY ");
            query.Append(string.Join(", ", OrderByColumns));
        }

        query.Append(" LIMIT @_batchSize");
        parameters.Add(new QueryParameter("_batchSize", BatchSize));

        return query.ToString();
    }

    private void AppendTupleCursor(
        StringBuilder query,
        List<IQueryParameter> parameters,
        object?[] values
    )
    {
        query.Append(" AND (");
        query.Append(string.Join(", ", OrderByColumns));
        query.Append(") > (");

        for (var i = 0; i < OrderByColumns.Length; i++)
        {
            if (i > 0)
                query.Append(", ");
            var paramName = "_cursor" + i;
            query.Append('@');
            query.Append(paramName);
            parameters.Add(new QueryParameter(paramName, values[i] ?? DBNull.Value));
        }

        query.Append(")");
    }

    private string QuotedTableRef()
    {
        var schema = string.IsNullOrWhiteSpace(Schema) ? "public" : Schema;
        return $"\"{schema}\".\"{TableName}\"";
    }

    private object?[] ReadCursorValues(IDataRecord reader)
    {
        var values = new object?[OrderByColumns.Length];
        for (var i = 0; i < OrderByColumns.Length; i++)
        {
            var ordinal = reader.GetOrdinal(OrderByColumns[i]);
            values[i] = reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal);
        }
        return values;
    }

    private CheckpointPayload? LoadCheckpoint(CancellationToken ct)
    {
        var json = CheckpointStore?.LoadAsync(ct).GetAwaiter().GetResult();
        if (json == null)
            return null;
        var payload = JsonSerializer.Deserialize<CheckpointPayload>(json);
        if (payload == null)
            return null;
        for (var i = 0; i < payload.Values.Length; i++)
        {
            if (payload.Values[i] is JsonElement element)
                payload.Values[i] = ConvertJsonElement(element);
        }
        return payload;
    }

    private static object? ConvertJsonElement(JsonElement element) =>
        element.ValueKind switch
        {
            JsonValueKind.Number when element.TryGetInt64(out var l) => l,
            JsonValueKind.Number => element.GetDouble(),
            JsonValueKind.String when element.TryGetDateTimeOffset(out var dto) => dto,
            JsonValueKind.String when element.TryGetGuid(out var g) => g,
            JsonValueKind.String => element.GetString(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            _ => null,
        };

    private void SaveCheckpoint(CheckpointPayload payload, CancellationToken ct)
    {
        if (CheckpointStore == null)
            return;
        var json = JsonSerializer.Serialize(payload);
        CheckpointStore.SaveAsync(json, ct).GetAwaiter().GetResult();
    }

    private sealed class CheckpointPayload
    {
        public object?[] Values { get; set; }

        [System.Text.Json.Serialization.JsonConstructor]
        public CheckpointPayload(object?[] values) => Values = values;
    }

    private sealed class QueryParameter : IQueryParameter
    {
        public string Name { get; }
        public string Type { get; } = string.Empty;
        public object Value { get; }
        public DbType DBType { get; }

        public QueryParameter(string name, object value)
        {
            Name = name;
            Value = value;
            DBType = MapDbType(value);
        }

        private static DbType MapDbType(object value) =>
            value switch
            {
                long or int or short => DbType.Int64,
                DateTime => DbType.DateTime2,
                DateTimeOffset => DbType.DateTimeOffset,
                string => DbType.String,
                Guid => DbType.Guid,
                _ => DbType.Object,
            };
    }
}
