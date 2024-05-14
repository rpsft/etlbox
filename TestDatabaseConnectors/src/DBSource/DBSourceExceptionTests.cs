using System.Threading;
using System.Threading.Tasks;
using ALE.ETLBox;
using ALE.ETLBox.DataFlow;
using Microsoft.Data.SqlClient;

namespace TestDatabaseConnectors.DBSource
{
    [Collection(nameof(DataFlowSourceDestinationCollection))]
    public class DbSourceExceptionTests : DatabaseConnectorsTestBase
    {
        public DbSourceExceptionTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        [Fact]
        public void UnknownTable()
        {
            //Arrange
            DbSource<string[]> source = new DbSource<string[]>(SqlConnection, "UnknownTable");
            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();

            //Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
            {
                source.LinkTo(dest);
                source.Execute(CancellationToken.None);
                dest.Wait();
            });
        }

        [Fact]
        public void UnknownTableViaTableDefinition()
        {
            //Arrange
            TableDefinition def = new TableDefinition(
                "UnknownTable",
                new List<TableColumn> { new("id", "INT") }
            );
            DbSource<string[]> source = new DbSource<string[]>
            {
                ConnectionManager = SqlConnection,
                SourceTableDefinition = def
            };
            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();

            //Act & Assert
            Assert.Throws<SqlException>(() =>
            {
                source.LinkTo(dest);
                source.Execute(CancellationToken.None);
                dest.Wait();
            });
        }

        [Fact]
        public async Task ErrorInSql()
        {
            //Arrange
            DbSource source = new DbSource(SqlConnection) { Sql = "SELECT XYZ FROM ABC" };
            MemoryDestination dest = new MemoryDestination();
            source.LinkTo(dest);
            //Act & Assert
            await Assert.ThrowsAsync<SqlException>(async () =>
            {
                await source.ExecuteAsync(CancellationToken.None);
                await dest.Completion;
            });
        }
    }
}
