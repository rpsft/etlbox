using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBSourceExceptionTests
    {
        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DBSourceExceptionTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void UnknownTable()
        {
            //Arrange
            DBSource source = new DBSource(SqlConnection, "UnknownTable");
            MemoryDestination dest = new MemoryDestination();

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
            TableDefinition def = new TableDefinition("UnknownTable",
                new List<TableColumn>()
                {
                    new TableColumn("id", "INT")
                });
            DBSource source = new DBSource()
            {
                ConnectionManager = SqlConnection,
                SourceTableDefinition = def
            };
            MemoryDestination dest = new MemoryDestination();

            //Act & Assert
            Assert.Throws<Microsoft.Data.SqlClient.SqlException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }
    }
}
