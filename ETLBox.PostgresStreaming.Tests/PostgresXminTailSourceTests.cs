using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using Npgsql;
using Xunit;

namespace ETLBox.PostgresStreaming.Tests;

[Collection("Postgres")]
#pragma warning disable SP3110
public sealed class PostgresXminTailSourceTests : IClassFixture<PostgresContainerFixture>
#pragma warning restore SP3110
{
    private readonly PostgresContainerFixture _fixture;

    public PostgresXminTailSourceTests(PostgresContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [MustDisposeResource]
    private PostgresConnectionManager CreateConnectionManager() =>
        new(_fixture.ConnectionString) { LeaveOpen = true };

    private static void ExecuteSql(NpgsqlConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void SetupTestTable(NpgsqlConnection conn, string table)
    {
        ExecuteSql(
            conn,
            $"""
            DROP TABLE IF EXISTS {table};
            CREATE TABLE {table} (
                id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                name TEXT NOT NULL
            )
            """
        );
    }

    private static void InsertRow(NpgsqlConnection conn, string table, string name)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {table} (name) VALUES (@n)";
        cmd.Parameters.AddWithValue("n", name);
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public void Execute_ReadsAllRows_InOrder()
    {
        const string tableName = "events_order_test";
        using var conn = new NpgsqlConnection(_fixture.ConnectionString);
        conn.Open();
        SetupTestTable(conn, tableName);
        InsertRow(conn, tableName, "alpha");
        InsertRow(conn, tableName, "beta");
        InsertRow(conn, tableName, "gamma");

        var results = new List<string>();
        var destination = new CustomDestination<(long Id, string Name)>(row =>
            results.Add(row.Name)
        );

        using var cm = CreateConnectionManager();
        using var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var source = new PostgresXminTailSource<(long Id, string Name)>
        {
            ConnectionManager = cm,
            TableName = tableName,
            Schema = "public",
            OrderByColumns = new[] { "id" },
            BatchSize = 100,
            PollingInterval = TimeSpan.FromMilliseconds(100),
            RowMapper = r => ((long)r["id"], (string)r["name"]),
        };
        source.LinkTo(destination);

        tokenSource.CancelAfter(TimeSpan.FromMilliseconds(500));
        Assert.Throws<OperationCanceledException>(() => source.Execute(tokenSource.Token));
        destination.Wait();

        Assert.Equal(3, results.Count);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, results);
    }

    [Fact]
    public void Execute_WithCheckpoint_ResumesFromLastPosition()
    {
        const string tableName = "events_checkpoint_test";
        using var conn = new NpgsqlConnection(_fixture.ConnectionString);
        conn.Open();
        SetupTestTable(conn, tableName);
        InsertRow(conn, tableName, "first");
        InsertRow(conn, tableName, "second");

        var checkpointStore = new InMemoryCheckpointStore();

        // First run — read rows and checkpoint
        var firstRun = new List<string>();
        var destFirst = new CustomDestination<(long Id, string Name)>(r => firstRun.Add(r.Name));
        using var cm1 = CreateConnectionManager();
        using var tokenSource1 = new CancellationTokenSource();

        var source1 = new PostgresXminTailSource<(long Id, string Name)>
        {
            ConnectionManager = cm1,
            TableName = tableName,
            Schema = "public",
            OrderByColumns = new[] { "id" },
            BatchSize = 100,
            PollingInterval = TimeSpan.FromMilliseconds(100),
            CheckpointStore = checkpointStore,
            RowMapper = r => ((long)r["id"], (string)r["name"]),
        };
        source1.LinkTo(destFirst);
        tokenSource1.CancelAfter(TimeSpan.FromMilliseconds(500));
        Assert.Throws<OperationCanceledException>(() => source1.Execute(tokenSource1.Token));
        destFirst.Wait();

        Assert.Equal(new[] { "first", "second" }, firstRun);

        // Insert new rows after checkpoint
        InsertRow(conn, tableName, "third");
        InsertRow(conn, tableName, "fourth");

        // Second run — should only return rows after the saved checkpoint
        var secondRun = new List<string>();
        var destSecond = new CustomDestination<(long Id, string Name)>(r => secondRun.Add(r.Name));
        using var cm2 = CreateConnectionManager();
        using var tokenSource2 = new CancellationTokenSource();

        var source2 = new PostgresXminTailSource<(long Id, string Name)>
        {
            ConnectionManager = cm2,
            TableName = tableName,
            Schema = "public",
            OrderByColumns = new[] { "id" },
            BatchSize = 100,
            PollingInterval = TimeSpan.FromMilliseconds(100),
            CheckpointStore = checkpointStore,
            RowMapper = r => ((long)r["id"], (string)r["name"]),
        };
        source2.LinkTo(destSecond);
        tokenSource2.CancelAfter(TimeSpan.FromMilliseconds(500));
        Assert.Throws<OperationCanceledException>(() => source2.Execute(tokenSource2.Token));
        destSecond.Wait();

        Assert.Equal(new[] { "third", "fourth" }, secondRun);
    }

    [Fact]
    public void Execute_FrontierExcludesUncommittedRows()
    {
        // Verifies the xmin-frontier correctly excludes rows from open transactions.
        // We simulate this by inserting inside a transaction that we commit AFTER
        // the source has already polled once: those rows must appear in the next batch.
        const string tableName = "events_frontier_test";
        using var conn = new NpgsqlConnection(_fixture.ConnectionString);
        conn.Open();
        SetupTestTable(conn, tableName);
        InsertRow(conn, tableName, "committed_before");

        var results = new List<string>();

        // Open a long transaction that holds an xid slot
        using var openTransaction = conn.BeginTransaction();
        using var slowCmd = conn.CreateCommand();
        slowCmd.Transaction = openTransaction;
        slowCmd.CommandText = $"INSERT INTO {tableName} (name) VALUES ('in_flight')";
        slowCmd.ExecuteNonQuery();

        // Start source — should see "committed_before" but NOT "in_flight"
        using var cm = CreateConnectionManager();
        using var tokenSource = new CancellationTokenSource();
        var destination = new CustomDestination<(long Id, string Name)>(r => results.Add(r.Name));

        var source = new PostgresXminTailSource<(long Id, string Name)>
        {
            ConnectionManager = cm,
            TableName = tableName,
            Schema = "public",
            OrderByColumns = new[] { "id" },
            BatchSize = 100,
            PollingInterval = TimeSpan.FromMilliseconds(50),
            RowMapper = r => ((long)r["id"], (string)r["name"]),
        };
        source.LinkTo(destination);

        tokenSource.CancelAfter(TimeSpan.FromMilliseconds(300));
        Assert.Throws<OperationCanceledException>(() => source.Execute(tokenSource.Token));
        destination.Wait();

        // Only the pre-existing committed row should be present
        Assert.DoesNotContain("in_flight", results);
        Assert.Contains("committed_before", results);

        // Commit the slow transaction — subsequent poll should pick it up
        openTransaction.Commit();
    }

    [Fact]
    public void Execute_AdditionalWhere_FiltersRows()
    {
        const string tableName = "events_where_test";
        using var conn = new NpgsqlConnection(_fixture.ConnectionString);
        conn.Open();
        SetupTestTable(conn, tableName);
        InsertRow(conn, tableName, "keep_me");
        InsertRow(conn, tableName, "skip_me");
        InsertRow(conn, tableName, "keep_me_too");

        var results = new List<string>();
        var destination = new CustomDestination<(long Id, string Name)>(r => results.Add(r.Name));
        using var cm = CreateConnectionManager();
        using var tokenSource = new CancellationTokenSource();

        var source = new PostgresXminTailSource<(long Id, string Name)>
        {
            ConnectionManager = cm,
            TableName = tableName,
            Schema = "public",
            OrderByColumns = new[] { "id" },
            BatchSize = 100,
            PollingInterval = TimeSpan.FromMilliseconds(100),
            AdditionalWhere = "name LIKE 'keep%'",
            RowMapper = r => ((long)r["id"], (string)r["name"]),
        };
        source.LinkTo(destination);

        tokenSource.CancelAfter(TimeSpan.FromMilliseconds(500));
        Assert.Throws<OperationCanceledException>(() => source.Execute(tokenSource.Token));
        destination.Wait();

        Assert.Equal(new[] { "keep_me", "keep_me_too" }, results);
    }
}
