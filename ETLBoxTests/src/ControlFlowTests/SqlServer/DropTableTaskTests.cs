using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("Sql Server ControlFlow")]
    public class DropTableTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public DropTableTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]
        public void DropTable()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create(Connection, "DropTableTest", columns);
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'U' AND object_id = object_id('DropTableTest')"));
            //Act
            DropTableTask.Drop(Connection, "DropTableTest");
            //Assert
            Assert.Equal(0, RowCountTask.Count(Connection, "sys.objects",
                 "type = 'U' AND object_id = object_id('DropTableTest')"));
        }
    }
}
