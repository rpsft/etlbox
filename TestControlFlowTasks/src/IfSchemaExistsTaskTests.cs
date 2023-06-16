using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    public class IfSchemaExistsTaskTests : ControlFlowTestBase
    {
        public IfSchemaExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLite;

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