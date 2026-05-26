# Streaming sources

Streaming sources continuously tail an external system and emit records into the data flow until
cancelled. They are designed for long-running, event-driven pipelines — unlike batch sources, they
never signal completion on their own and must be stopped via a `CancellationToken`.

Both sources described here support **resumable processing** through the `ICheckpointStore`
abstraction, which persists the last-seen cursor so a restart picks up exactly where it left off.

## ICheckpointStore

`ICheckpointStore` is defined in `ETLBox.Common` and decouples the checkpoint format from the
storage backend:

```csharp
public interface ICheckpointStore
{
    Task<string?> LoadAsync(CancellationToken ct);
    Task SaveAsync(string payload, CancellationToken ct);
}
```

The payload is an opaque JSON string; each source type defines its own format.
Implement this interface to persist cursors in Redis, a database, a file, or any other store.

> **Important:** each source instance must have its own `ICheckpointStore`. Two sources sharing one
> store will overwrite each other's cursor.

## MongoChangeStreamSource (ETLBox.MongoDB)

`MongoChangeStreamSource<TOutput>` watches a MongoDB
[change stream](https://www.mongodb.com/docs/manual/changeStreams/) and emits one record per change
event.

### Requirements

- The MongoDB deployment must run in **replica set mode** (a single-node replica set is sufficient
  for development).
- NuGet package: `EtlBox.MongoDB`

### Basic usage

```csharp
using var tokenSource = new CancellationTokenSource();

var source = new MongoChangeStreamSource<MyEvent>
{
    MongoClient = new MongoClient("mongodb://localhost:27017/?replicaSet=rs0"),
    Database = "mydb",
    Collection = "orders",
    EventMapper = doc => new MyEvent
    {
        Id    = doc.FullDocument["_id"].AsObjectId.ToString(),
        Total = doc.FullDocument["total"].AsDouble,
    },
};

source.LinkTo(destination);
source.Execute(tokenSource.Token);
```

### Filtering with a pipeline

Pass a MongoDB aggregation pipeline to receive only the events you care about:

```csharp
var filterStage = new BsonDocumentPipelineStageDefinition<
    ChangeStreamDocument<BsonDocument>,
    ChangeStreamDocument<BsonDocument>
>(BsonDocument.Parse("{ $match: { 'fullDocument.status': 'shipped' } }"));

var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
    .AppendStage(filterStage);

var source = new MongoChangeStreamSource<MyEvent>
{
    MongoClient  = client,
    Database     = "mydb",
    Collection   = "orders",
    Pipeline     = pipeline,
    EventMapper  = doc => /* ... */,
};
```

### Resuming after restart

Provide a `CheckpointStore` to resume from the last processed event:

```csharp
var source = new MongoChangeStreamSource<MyEvent>
{
    MongoClient     = client,
    Database        = "mydb",
    Collection      = "orders",
    CheckpointStore = new MyRedisCheckpointStore("orders-cursor"),
    EventMapper     = doc => /* ... */,
};
```

The source saves a MongoDB resume token after each batch. On restart it passes that token to
`Watch()`, so no events are lost and no events are replayed.

### Key properties

| Property | Default | Description |
|---|---|---|
| `MongoClient` | required | MongoDB client instance (caller manages lifetime) |
| `Database` | required | Database name |
| `Collection` | required | Collection name |
| `EventMapper` | required | Maps `ChangeStreamDocument<BsonDocument>` to `TOutput` |
| `Pipeline` | `null` (all events) | Aggregation pipeline for server-side filtering |
| `FullDocument` | `UpdateLookup` | Controls which document version is returned on updates |
| `MaxAwaitTime` | 1 second | Max time the server waits for new events per batch |
| `CheckpointStore` | `null` (start from now) | Resume token persistence |

---

## PostgresXminTailSource (ETLBox.PostgresStreaming)

`PostgresXminTailSource<TOutput>` continuously polls a PostgreSQL table for new rows using the
`xmin` system column as a visibility fence. It uses `pg_snapshot_xmin(pg_current_snapshot())` as a
read-safe frontier, which guarantees that rows from in-flight transactions are never emitted
prematurely — they appear only after their transaction commits.

### Requirements

- PostgreSQL 13 or later (uses `pg_current_snapshot()`).
- NuGet package: `EtlBox.PostgresStreaming`

### Basic usage

```csharp
using var tokenSource = new CancellationTokenSource();
using var cm = new PostgresConnectionManager("Host=localhost;Database=mydb;Username=etl;Password=secret")
{
    LeaveOpen = true,
};

var source = new PostgresXminTailSource<MyRow>
{
    ConnectionManager = cm,
    TableName         = "orders",
    Schema            = "public",
    OrderByColumns    = new[] { "id" },
    RowMapper         = r => new MyRow { Id = (long)r["id"], Name = (string)r["name"] },
};

source.LinkTo(destination);
source.Execute(tokenSource.Token);
```

### Resuming after restart

```csharp
var source = new PostgresXminTailSource<MyRow>
{
    ConnectionManager = cm,
    TableName         = "orders",
    Schema            = "public",
    OrderByColumns    = new[] { "id" },
    CheckpointStore   = new MyFileCheckpointStore("orders.cursor"),
    RowMapper         = r => /* ... */,
};
```

The source checkpoints the cursor values of the last row in every non-empty batch. On restart it
resumes with a tuple-cursor `WHERE (id) > (@last_id)` clause so no rows are skipped or duplicated.

### Filtering rows

Use `AdditionalWhere` to apply a server-side predicate on top of the xmin fence:

```csharp
var source = new PostgresXminTailSource<MyRow>
{
    // ...
    AdditionalWhere = "status = 'pending'",
};
```

### Key properties

| Property | Default | Description |
|---|---|---|
| `ConnectionManager` | required | Postgres connection manager |
| `TableName` | required | Table to poll (must expose the `xmin` system column) |
| `Schema` | `"public"` | Schema that contains the table |
| `OrderByColumns` | required | Columns used for ordering and cursor pagination |
| `RowMapper` | required | Maps `IDataRecord` to `TOutput` |
| `BatchSize` | 500 | Rows per polling round |
| `PollingInterval` | 1 second | Pause between rounds when no rows are found |
| `AdditionalWhere` | `null` | Extra SQL predicate appended with AND |
| `CheckpointStore` | `null` (start from beginning) | Cursor persistence |
