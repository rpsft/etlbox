using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.DataFlow;
using ALE.ETLBox.src.Toolbox.Logging;
using ALE.ETLBoxTests.NonParallel.src.Fixtures;
using EtlBox.Logging.Database;

namespace ALE.ETLBoxTests.NonParallel.src.ControlFlow
{
    public sealed class DefaultDbConnectionTests : NonParallelTestBase, IDisposable
    {
        public DefaultDbConnectionTests(LoggingDatabaseFixture fixture)
            : base(fixture)
        {
            CreateLogTableTask.Create(SqlConnection);
            ETLBox.src.Toolbox.ControlFlow.ControlFlow.DefaultDbConnection = SqlConnection;
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable);
            ETLBox.src.Toolbox.ControlFlow.ControlFlow.ClearSettings();
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
            var source = new DbSource<MySimpleRow>("TestSourceTable");
            var dest = new DbDestination<MySimpleRow>(
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
