using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class DropTableTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public DropTableTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void DropTable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create(connection, "DropTableTest", columns);
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "DropTableTest"));

            //Act
            DropTableTask.Drop(connection, "DropTableTest");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(connection, "DropTableTest"));
        }
    }
}
