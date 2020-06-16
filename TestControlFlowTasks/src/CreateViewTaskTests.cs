using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateViewTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> Access => Config.AccessConnection("ControlFlow");

        public CreateViewTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))
              , MemberData(nameof(Access))]
        public void CreateView(IConnectionManager connection)
        {
            //Arrange
            string viewtext = "SELECT 1 AS test";
            if (connection.GetType() == typeof(OracleConnectionManager))
                viewtext += " FROM DUAL";
            //Act
            CreateViewTask.CreateOrAlter(connection, "View1", viewtext);
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View1"));
            var td = TableDefinition.FromTableName(connection, "View1");
            Assert.Contains(td.Columns, col => col.Name.ToLower() == "test");
        }

        [Theory, MemberData(nameof(Connections))]
        public void AlterView(IConnectionManager connection)
        {
            //Arrange
            string viewtext = "SELECT 1 AS Test";
            if (connection.GetType() == typeof(OracleConnectionManager))
                viewtext += " FROM DUAL";

            CreateViewTask.CreateOrAlter(connection, "View2", viewtext);
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View2"));

            //Act
            CreateViewTask.CreateOrAlter(connection, "View2", viewtext);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View2"));
        }
    }
}
