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
    public class DropViewTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public DropViewTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void DropView(IConnectionManager connection)
        {
            //Arrange
            CreateViewTask.CreateOrAlter(connection, "DropViewTest", "SELECT 1 AS Test");
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "DropViewTest"));

            //Act
            DropViewTask.Drop(connection, "DropViewTest");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(connection, "DropTableTest"));
        }
    }
}
