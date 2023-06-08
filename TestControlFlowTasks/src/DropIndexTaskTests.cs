using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class DropIndexTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        [Theory, MemberData(nameof(Connections))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            CreateTableTask.Create(
                connection,
                "DropIndexTable",
                new List<TableColumn> { new("Test1", "INT") }
            );
            CreateIndexTask.CreateOrRecreate(
                connection,
                "IndexToDrop",
                "DropIndexTable",
                new List<string> { "Test1" }
            );
            Assert.True(IfIndexExistsTask.IsExisting(connection, "IndexToDrop", "DropIndexTable"));

            //Act
            DropIndexTask.Drop(connection, "IndexToDrop", "DropIndexTable");

            //Assert
            Assert.False(IfIndexExistsTask.IsExisting(connection, "IndexToDrop", "DropIndexTable"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void DropIfExists(IConnectionManager connection)
        {
            //Arrange
            DropIndexTask.DropIfExists(connection, "IndexIfExists", "DropIfExistsIndexTable");
            CreateTableTask.Create(
                connection,
                "DropIfExistsIndexTable",
                new List<TableColumn> { new("Test1", "INT") }
            );
            CreateIndexTask.CreateOrRecreate(
                connection,
                "IndexIfExists",
                "DropIfExistsIndexTable",
                new List<string> { "Test1" }
            );
            Assert.True(
                IfIndexExistsTask.IsExisting(connection, "IndexIfExists", "DropIfExistsIndexTable")
            );

            //Act
            DropIndexTask.DropIfExists(connection, "IndexIfExists", "DropIfExistsIndexTable");

            //Assert
            Assert.False(
                IfIndexExistsTask.IsExisting(connection, "IndexIfExists", "DropIfExistsIndexTable")
            );
        }
    }
}
