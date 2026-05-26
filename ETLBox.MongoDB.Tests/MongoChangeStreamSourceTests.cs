using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

// ReSharper disable All

namespace ETLBox.MongoDB.Tests;

[Collection("MongoDB")]
public sealed class MongoChangeStreamSourceTests : IClassFixture<MongoContainerFixture>
{
    private const string DatabaseName = "etltest";
    private readonly MongoContainerFixture _fixture;

    public MongoChangeStreamSourceTests(MongoContainerFixture fixture)
    {
        _fixture = fixture;
    }

    private IMongoClient CreateClient() => new MongoClient(_fixture.ConnectionString);

    private static IMongoCollection<BsonDocument> GetCollection(IMongoClient client, string name)
    {
        var db = client.GetDatabase(DatabaseName);
        var collection = db.GetCollection<BsonDocument>(name);
        collection.DeleteMany(FilterDefinition<BsonDocument>.Empty);
        return collection;
    }

    private static void WaitForResults<T>(List<T> results, int expectedCount, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (results.Count < expectedCount && DateTime.UtcNow < deadline)
            Thread.Sleep(30);
    }

    [Fact]
    public async Task Execute_ReceivesInsertedDocuments_InOrder()
    {
        var client = CreateClient();
        var collection = GetCollection(client, "change_stream_basic");

        var results = new List<string>();
        var destination = new CustomDestination<string>(name => results.Add(name));

        using var tokenSource = new CancellationTokenSource();
        var source = new MongoChangeStreamSource<string>
        {
            MongoClient = client,
            Database = DatabaseName,
            Collection = "change_stream_basic",
            MaxAwaitTime = TimeSpan.FromMilliseconds(200),
            EventMapper = doc => doc.FullDocument["name"].AsString,
        };
        source.LinkTo(destination);

        // ReSharper disable once AccessToDisposedClosure
        var executeTask = Task.Run(() => source.Execute(tokenSource.Token), CancellationToken.None);

        // Allow the cursor to open before inserting
        await Task.Delay(500, CancellationToken.None).ConfigureAwait(true);

        await collection
            .InsertOneAsync(new BsonDocument { { "name", "alpha" } }, null, CancellationToken.None)
            .ConfigureAwait(true);
        await collection.InsertOneAsync(
            new BsonDocument { { "name", "beta" } },
            null,
            CancellationToken.None
        );
        await collection.InsertOneAsync(
            new BsonDocument { { "name", "gamma" } },
            null,
            CancellationToken.None
        );

        WaitForResults(results, 3, TimeSpan.FromSeconds(15));
        await tokenSource.CancelAsync();

        Assert.Throws<OperationCanceledException>(() => executeTask.GetAwaiter().GetResult());
        destination.Wait();

        Assert.Equal(3, results.Count);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, results);
    }

    [Fact]
    public async Task Execute_WithCheckpoint_ResumesAfterToken()
    {
        var client = CreateClient();
        var collection = GetCollection(client, "change_stream_checkpoint");

        var checkpointStore = new InMemoryCheckpointStore();

        // First run — receive two inserts and checkpoint
        var firstRun = new List<string>();
        var destFirst = new CustomDestination<string>(name => firstRun.Add(name));

        using var tokenSource1 = new CancellationTokenSource();
        var source1 = new MongoChangeStreamSource<string>
        {
            MongoClient = client,
            Database = DatabaseName,
            Collection = "change_stream_checkpoint",
            MaxAwaitTime = TimeSpan.FromMilliseconds(200),
            CheckpointStore = checkpointStore,
            EventMapper = doc => doc.FullDocument["name"].AsString,
        };
        source1.LinkTo(destFirst);

        // ReSharper disable once AccessToDisposedClosure
        var task1 = Task.Run(() => source1.Execute(tokenSource1.Token));
        await Task.Delay(500).ConfigureAwait(true);

        await collection.InsertOneAsync(new BsonDocument { { "name", "first" } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "second" } });

        WaitForResults(firstRun, 2, TimeSpan.FromSeconds(15));
        await tokenSource1.CancelAsync();

        Assert.Throws<OperationCanceledException>(() => task1.GetAwaiter().GetResult());
        destFirst.Wait();

        Assert.Equal(new[] { "first", "second" }, firstRun);

        // Insert new documents after checkpoint was saved
        await collection.InsertOneAsync(new BsonDocument { { "name", "third" } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "fourth" } });

        // Second run — resume from checkpoint, should receive only new documents
        var secondRun = new List<string>();
        var destSecond = new CustomDestination<string>(name => secondRun.Add(name));

        using var tokenSource2 = new CancellationTokenSource();
        var source2 = new MongoChangeStreamSource<string>
        {
            MongoClient = client,
            Database = DatabaseName,
            Collection = "change_stream_checkpoint",
            MaxAwaitTime = TimeSpan.FromMilliseconds(200),
            CheckpointStore = checkpointStore,
            EventMapper = doc => doc.FullDocument["name"].AsString,
        };
        source2.LinkTo(destSecond);

        var task2 = Task.Run(() => source2.Execute(tokenSource2.Token));

        WaitForResults(secondRun, 2, TimeSpan.FromSeconds(15));
        await tokenSource2.CancelAsync();

        Assert.Throws<OperationCanceledException>(() => task2.GetAwaiter().GetResult());
        destSecond.Wait();

        Assert.Equal(new[] { "third", "fourth" }, secondRun);
    }

    [Fact]
    public async Task Execute_WithPipeline_FiltersEvents()
    {
        var client = CreateClient();
        var collection = GetCollection(client, "change_stream_pipeline");

        var filterStage = new BsonDocumentPipelineStageDefinition<
            ChangeStreamDocument<BsonDocument>,
            ChangeStreamDocument<BsonDocument>
        >(BsonDocument.Parse("{ $match: { 'fullDocument.keep': true } }"));
        var pipeline = new EmptyPipelineDefinition<
            ChangeStreamDocument<BsonDocument>
        >().AppendStage(filterStage);

        var results = new List<string>();
        var destination = new CustomDestination<string>(name => results.Add(name));

        using var tokenSource = new CancellationTokenSource();
        var source = new MongoChangeStreamSource<string>
        {
            MongoClient = client,
            Database = DatabaseName,
            Collection = "change_stream_pipeline",
            MaxAwaitTime = TimeSpan.FromMilliseconds(200),
            Pipeline = pipeline,
            EventMapper = doc => doc.FullDocument["name"].AsString,
        };
        source.LinkTo(destination);

        var executeTask = Task.Run(() => source.Execute(tokenSource.Token));
        await Task.Delay(500);

        await collection.InsertOneAsync(
            new BsonDocument { { "name", "keep_me" }, { "keep", true } }
        );
        await collection.InsertOneAsync(
            new BsonDocument { { "name", "skip_me" }, { "keep", false } }
        );
        await collection.InsertOneAsync(
            new BsonDocument { { "name", "keep_too" }, { "keep", true } }
        );

        WaitForResults(results, 2, TimeSpan.FromSeconds(15));
        await tokenSource.CancelAsync();

        Assert.Throws<OperationCanceledException>(() => executeTask.GetAwaiter().GetResult());
        destination.Wait();

        Assert.Equal(new[] { "keep_me", "keep_too" }, results);
    }
}
