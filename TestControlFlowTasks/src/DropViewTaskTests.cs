using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class DropViewTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public DropViewTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            string viewtext = "SELECT 1 AS test";
            if (connection.GetType() == typeof(OracleConnectionManager))
                viewtext += " FROM DUAL";
            CreateViewTask.CreateOrAlter(connection, "DropViewTest", viewtext);
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "DropViewTest"));

            //Act
            DropViewTask.Drop(connection, "DropViewTest");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(connection, "DropTableTest"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void DropIfExists(IConnectionManager connection)
        {
            //Arrange
            string viewtext = "SELECT 1 AS test";
            if (connection.GetType() == typeof(OracleConnectionManager))
                viewtext += " FROM DUAL";

            // Act
            DropViewTask.DropIfExists(connection, "DropIfExistsViewTest");

            //Arrange
            CreateViewTask.CreateOrAlter(connection, "DropIfExistsViewTest", viewtext);
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "DropIfExistsViewTest"));

            //Act
            DropViewTask.DropIfExists(connection, "DropIfExistsViewTest");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(connection, "DropIfExistsViewTest"));
        }
    }
}
