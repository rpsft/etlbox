using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using EtlBox.Logging.Database;
using TestNonParallel.Fixtures;

namespace TestNonParallel.ControlFlow
{
    [Collection("Logging")]
    public sealed class DefaultDbConnectionTests : NonParallelTestBase, IDisposable
    {
        public DefaultDbConnectionTests(LoggingDatabaseFixture fixture)
            : base(fixture)
        {
            CreateLogTableTask.Create(SqlConnection);
            ALE.ETLBox.Common.ControlFlow.ControlFlow.DefaultDbConnection = SqlConnection;
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ALE.ETLBox.Common.ControlFlow.ControlFlow.LogTable);
            ALE.ETLBox.Common.ControlFlow.ControlFlow.ClearSettings();
        }

        [Fact]
        public void CreateTableWithDefaultConnection()
        {
            //Arrange
            //Act
            CreateTableTask.Create("TestTable", new List<TableColumn> { new("value", "INT") });
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting("TestTable"));
        }

        [Fact]
        public void CreateSchemaWithDefaultConnection()
        {
            //Arrange
            //Act
            CreateSchemaTask.Create("testschema");
            //Assert
            Assert.True(IfSchemaExistsTask.IsExisting("testschema"));
        }

        public class MySimpleRow
        {
            public string Col1 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithDefaultConnection()
        {
            //Arrange
            CreateTableTask.Create(
                "TestSourceTable",
                new List<TableColumn> { new("Col1", "VARCHAR(100)") }
            );
            SqlTask.ExecuteNonQuery(
                "Insert test data",
                "INSERT INTO TestSourceTable VALUES ('T');"
            );
            CreateTableTask.Create(
                "TestDestinationTable",
                new List<TableColumn> { new("Col1", "VARCHAR(100)") }
            );
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("TestSourceTable");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                "TestDestinationTable"
            );

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(1, RowCountTask.Count("TestDestinationTable"));
        }
    }
}
