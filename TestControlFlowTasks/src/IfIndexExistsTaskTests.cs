using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    public class IfIndexExistsTaskTests : ControlFlowTestBase
    {
        public IfIndexExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        [Theory, MemberData(nameof(Connections))]
        public void IfIndexExists(IConnectionManager connection)
        {
            //Arrange
            SqlTask.ExecuteNonQuery(
                connection,
                "Create index test table",
                @"CREATE TABLE indextable (col1 INT NULL)"
            );

            //Act
            var existsBefore = IfIndexExistsTask.IsExisting(connection, "index_test", "indextable");

            SqlTask.ExecuteNonQuery(
                connection,
                "Create test index",
                @"CREATE INDEX index_test ON indextable (col1)"
            );
            var existsAfter = IfIndexExistsTask.IsExisting(connection, "index_test", "indextable");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }
    }
}
