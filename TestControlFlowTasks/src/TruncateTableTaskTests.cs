using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestControlFlowTasks.src
{
    public class TruncateTableTaskTests : ControlFlowTestBase
    {
        public TruncateTableTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public static IEnumerable<object[]> Access => AccessConnection;

        [Theory, MemberData(nameof(Connections)), MemberData(nameof(Access))]
        public void Truncate(IConnectionManager connection)
        {
            //Arrange
            var tableDef = new TwoColumnsTableFixture(
                connection,
                "TruncateTableTest"
            );
            tableDef.InsertTestData();
            Assert.Equal(3, RowCountTask.Count(connection, "TruncateTableTest"));
            //Act
            TruncateTableTask.Truncate(connection, "TruncateTableTest");
            //Assert
            Assert.Equal(0, RowCountTask.Count(connection, "TruncateTableTest"));
        }
    }
}
