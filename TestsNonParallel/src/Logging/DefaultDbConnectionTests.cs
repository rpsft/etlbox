using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class DefaultDbConnectionTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public DefaultDbConnectionTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTablesTask.CreateLog(Connection);
            ControlFlow.CurrentDbConnection = Connection;
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
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
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>("TestSourceTable");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>("TestDestinationTable");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(RowCountTask.Count("TestDestinationTable").Value == 1);
        }

    }
}
