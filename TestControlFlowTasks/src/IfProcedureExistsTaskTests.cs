using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class IfProcedureExistsTaskTests : ControlFlowTestBase
    {
        public IfProcedureExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLiteAndClickHouse;

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
