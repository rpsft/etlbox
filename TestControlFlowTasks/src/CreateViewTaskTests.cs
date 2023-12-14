using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class CreateViewTaskTests : ControlFlowTestBase
    {
        public CreateViewTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(AllSqlConnections)), MemberData(nameof(AccessConnection))]
        public void CreateView(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateViewTask.CreateOrAlter(connection, "View1", "SELECT 1 AS test");
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View1"));
            var td = TableDefinition.GetDefinitionFromTableName(connection, "View1");
            Assert.Contains(td.Columns, col => col.Name == "test");
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void AlterView(IConnectionManager connection)
        {
            //Arrange
            CreateViewTask.CreateOrAlter(connection, "View2", "SELECT 1 AS Test");
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View2"));

            //Act
            CreateViewTask.CreateOrAlter(connection, "View2", "SELECT 5 AS test");

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "View2"));
        }
    }
}
