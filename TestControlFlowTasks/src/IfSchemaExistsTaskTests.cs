using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class IfSchemaExistsTaskTests
    {
        public static IEnumerable<object[]> Connections =>
            Config.AllConnectionsWithoutSQLite("ControlFlow");

        [Theory, MemberData(nameof(Connections))]
        public void IfSchemaeExists(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager))
            {
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
}
