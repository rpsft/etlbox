using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class IfSchemaExistsTaskTests : ControlFlowTestBase
    {
        public IfSchemaExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLiteAndClickHouse;

        [Theory, MemberData(nameof(Connections))]
        public void IfSchemaExists(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            //Arrange
            var existsBefore = IfSchemaExistsTask.IsExisting(connection, "testschema");
            CreateSchemaTask.Create(connection, "testschema");

            //Act
            var existsAfter = IfSchemaExistsTask.IsExisting(connection, "testschema");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }
    }
}
