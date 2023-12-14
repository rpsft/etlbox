using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src;
using TestDatabaseConnectors.src.Fixtures;

namespace TestDatabaseConnectors.src.DBDestination
{
    public class DbDestinationExceptionTests : DatabaseConnectorsTestBase
    {
        public DbDestinationExceptionTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        [Fact]
        public void UnknownTable()
        {
            //Arrange
            string[] data = { "1", "2" };
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(data);
            var dest = new DbDestination<string[]>(
                SqlConnection,
                "UnknownTable"
            );
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                try
                {
                    source.Execute();
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException!;
                }
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

            //Arrange
            string[] data = { "1", "2" };
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(data);
            var dest = new DbDestination<string[]>
            {
                ConnectionManager = SqlConnection,
                DestinationTableDefinition = def
            };
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
            {
                try
                {
                    source.Execute();
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException!;
                }
            });
        }
    }
}
