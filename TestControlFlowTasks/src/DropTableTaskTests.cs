using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class DropTableTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> Access => Config.AccessConnection("ControlFlow");

        [Theory, MemberData(nameof(Connections)), MemberData(nameof(Access))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn> { new("value", "int") };
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
            List<TableColumn> columns = new List<TableColumn> { new("value", "int") };
            CreateTableTask.Create(connection, "DropIfExistsTableTest", columns);
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "DropIfExistsTableTest"));

            //Act
            DropTableTask.DropIfExists(connection, "DropIfExistsTableTest");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(connection, "DropIfExistsTableTest"));
        }
    }
}
