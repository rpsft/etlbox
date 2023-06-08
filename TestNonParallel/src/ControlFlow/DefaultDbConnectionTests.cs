using System.Collections.Generic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using TestShared.Helper;

namespace ALE.ETLBoxTests.NonParallel.ControlFlow
{
    [Collection("Logging")]
    public class DefaultDbConnectionTests : IDisposable
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("Logging");

        public DefaultDbConnectionTests()
        {
            CreateLogTableTask.Create(SqlConnection);
            ETLBox.ControlFlow.ControlFlow.DefaultDbConnection = SqlConnection;
            ETLBox.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ETLBox.ControlFlow.ControlFlow.LogTable);
            ETLBox.ControlFlow.ControlFlow.ClearSettings();
        }

        [Fact]
        public void CreateTableWithDefaultConnection()
        {
            //Arrange
            //Act
            CreateTableTask.Create(
                "TestTable",
                new List<TableColumn> { new TableColumn("value", "INT") }
            );
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
                new List<TableColumn> { new TableColumn("Col1", "VARCHAR(100)") }
            );
            SqlTask.ExecuteNonQuery(
                "Insert test data",
                "INSERT INTO TestSourceTable VALUES ('T');"
            );
            CreateTableTask.Create(
                "TestDestinationTable",
                new List<TableColumn> { new TableColumn("Col1", "VARCHAR(100)") }
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
