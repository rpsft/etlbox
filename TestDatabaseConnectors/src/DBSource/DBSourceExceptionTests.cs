using System.Threading.Tasks;
using ALE.ETLBox;
using ALE.ETLBox.Common;
using ALE.ETLBox.DataFlow;
using Microsoft.Data.SqlClient;
using TestDatabaseConnectors.Fixtures;

namespace TestDatabaseConnectors.DBSource
{
    public class DbSourceExceptionTests : DatabaseConnectorsTestBase
    {
        public DbSourceExceptionTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        [Fact]
        public void UnknownTable()
        {
            //Arrange
            var source = new DbSource<string[]>(SqlConnection, "UnknownTable");
            var dest = new MemoryDestination<string[]>();

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void UnknownTableViaTableDefinition()
        {
            //Arrange
            var def = new TableDefinition(
                "UnknownTable",
                new List<TableColumn> { new("id", "INT") }
            );
            var source = new DbSource<string[]>
            {
                ConnectionManager = SqlConnection,
                SourceTableDefinition = def
            };
            var dest = new MemoryDestination<string[]>();

            //Act & Assert
            Assert.Throws<SqlException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void ErrorInSql()
        {
            //Arrange
            var source = new DbSource(SqlConnection) { Sql = "SELECT XYZ FROM ABC" };
            var dest = new MemoryDestination();
            source.LinkTo(dest);
            //Act & Assert
            Assert.Throws<SqlException>(() =>
            {
                try
                {
                    Task s = source.ExecuteAsync();
                    Task c = dest.Completion;
                    Task.WaitAll(c, s);
                }
                catch (AggregateException e)
                {
                    throw e.InnerException!;
                }
            });
        }
    }
}
