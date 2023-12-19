using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    public class IfProcedureExistsTaskTests : ControlFlowTestBase
    {
        public IfProcedureExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLite;

        [Theory, MemberData(nameof(Connections))]
        public void IfProcedureExists(IConnectionManager connection)
        {
            //Arrange
            var existsBefore = IfProcedureExistsTask.IsExisting(connection, "sp_test");
            CreateProcedureTask.CreateOrAlter(connection, "sp_test", "SELECT 1;");

            //Act
            var existsAfter = IfProcedureExistsTask.IsExisting(connection, "sp_test");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }
    }
}
