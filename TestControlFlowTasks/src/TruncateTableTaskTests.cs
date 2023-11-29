using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;
using TestShared.SharedFixtures;

namespace TestControlFlowTasks
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
            TwoColumnsTableFixture tableDef = new TwoColumnsTableFixture(
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
