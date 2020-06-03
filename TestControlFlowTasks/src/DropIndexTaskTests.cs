using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class DropIndexTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public DropIndexTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            CreateTableTask.Create(connection, "DropIndexTable", new List<TableColumn>()
            {
                new TableColumn("Test1", "INT")
            });
            CreateIndexTask.CreateOrRecreate(connection, "IndexToDrop", "DropIndexTable",
                new List<string>() { "Test1" });
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
            CreateTableTask.Create(connection, "DropIfExistsIndexTable", new List<TableColumn>()
            {
                new TableColumn("Test1", "INT")
            });
            CreateIndexTask.CreateOrRecreate(connection, "IndexIfExists", "DropIfExistsIndexTable",
                new List<string>() { "Test1" });
            Assert.True(IfIndexExistsTask.IsExisting(connection, "IndexIfExists", "DropIfExistsIndexTable"));

            //Act
            DropIndexTask.DropIfExists(connection, "IndexIfExists", "DropIfExistsIndexTable");

            //Assert
            Assert.False(IfIndexExistsTask.IsExisting(connection, "IndexIfExists", "DropIfExistsIndexTable"));
        }
    }
}
