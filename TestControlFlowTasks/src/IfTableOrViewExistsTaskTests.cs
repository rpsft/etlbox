using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Toolbox.ConnectionManager.Odbc;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class IfTableOrViewExistsTaskTests : ControlFlowTestBase
    {
        public IfTableOrViewExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public static IEnumerable<object[]> Access => AccessConnection;

        [Theory, MemberData(nameof(Connections)), MemberData(nameof(Access))]
        public void IfTableExists(IConnectionManager connection)
        {
            //Arrange
            if (connection.GetType() != typeof(AccessOdbcConnectionManager))
                SqlTask.ExecuteNonQuery(
                    connection,
                    "Drop table if exists",
                    @"DROP TABLE IF EXISTS existtable_test"
                );

            //Act
            var existsBefore = IfTableOrViewExistsTask.IsExisting(connection, "existtable_test");

            SqlTask.ExecuteNonQuery(
                connection,
                "Create test data table",
                @"CREATE TABLE existtable_test ( Col1 INT NULL )"
            );
            var existsAfter = IfTableOrViewExistsTask.IsExisting(connection, "existtable_test");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }

        [Theory, MemberData(nameof(Connections)), MemberData(nameof(Access))]
        public void IfViewExists(IConnectionManager connection)
        {
            //Arrange
            if (connection.GetType() != typeof(AccessOdbcConnectionManager))
                SqlTask.ExecuteNonQuery(
                    connection,
                    "Drop view if exists",
                    @"DROP VIEW IF EXISTS existview_test"
                );

            //Act
            var existsBefore = IfTableOrViewExistsTask.IsExisting(connection, "existview_test");

            SqlTask.ExecuteNonQuery(
                connection,
                "Create test data table",
                @"CREATE VIEW existview_test AS SELECT 1 AS test"
            );
            var existsAfter = IfTableOrViewExistsTask.IsExisting(connection, "existview_test");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }

        [Theory, MemberData(nameof(Connections))]
        public void ThrowException(IConnectionManager connection)
        {
            //Arrange
            //Act
            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                IfTableOrViewExistsTask.ThrowExceptionIfNotExists(
                    connection,
                    "xyz.Somestrangenamehere"
                );
            });
        }
    }
}
