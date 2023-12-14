using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class DropTableTaskTests : ControlFlowTestBase
    {
        public DropTableTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public static IEnumerable<object[]> Access => AccessConnection;

        [Theory, MemberData(nameof(Connections)), MemberData(nameof(Access))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            var columns = new List<TableColumn> { new("value", "int") };
            CreateTableTask.Create(connection, "DropTableTest", columns);
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "DropTableTest"));

            //Act
            DropTableTask.Drop(connection, "DropTableTest");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(connection, "DropTableTest"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void DropIfExists(IConnectionManager connection)
        {
            //Arrange
            DropTableTask.DropIfExists(connection, "DropIfExistsTableTest");
            var columns = new List<TableColumn> { new("value", "int") };
            CreateTableTask.Create(connection, "DropIfExistsTableTest", columns);
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "DropIfExistsTableTest"));

            //Act
            DropTableTask.DropIfExists(connection, "DropIfExistsTableTest");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(connection, "DropIfExistsTableTest"));
        }
    }
}
