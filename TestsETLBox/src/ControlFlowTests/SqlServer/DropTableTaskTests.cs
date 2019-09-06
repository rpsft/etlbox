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
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public DropTableTaskTests(DatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void DropTable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create(connection, "DropTableTest", columns);
            Assert.True(IfExistsTask.IsExisting(connection, "DropTableTest"));

            //Act
            DropTableTask.Drop(connection, "DropTableTest");

            //Assert
            Assert.False(IfExistsTask.IsExisting(connection, "DropTableTest"));
        }
    }
}
