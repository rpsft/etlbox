using ETLBox;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.Logging;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class DefaultDbConnectionTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Logging");

        public DefaultDbConnectionTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTableTask.Create(SqlConnection);
            ControlFlow.DefaultDbConnection = SqlConnection;
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ControlFlow.LogTable);
            ControlFlow.ClearSettings();
        }


        [Fact]
        public void CreateTableWithDefaultConnection()
        {
            //Arrange
            //Act
            CreateTableTask.Create("TestTable",
                new List<TableColumn>() { new TableColumn("value", "INT") });
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
            CreateTableTask.Create("TestSourceTable",
                new List<TableColumn>() { new TableColumn("Col1", "VARCHAR(100)") });
            SqlTask.ExecuteNonQuery("Insert test data", "INSERT INTO TestSourceTable VALUES ('T');");
            CreateTableTask.Create("TestDestinationTable",
                new List<TableColumn>() { new TableColumn("Col1", "VARCHAR(100)") });
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("TestSourceTable");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("TestDestinationTable");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(RowCountTask.Count("TestDestinationTable").Value == 1);
        }

    }
}
