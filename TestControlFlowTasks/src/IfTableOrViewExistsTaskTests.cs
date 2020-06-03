using ETLBox;
using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Exceptions;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class IfTableOrViewExistsTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> Access => Config.AccessConnection("ControlFlow");

        public IfTableOrViewExistsTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))
               , MemberData(nameof(Access))]
        public void IfTableExists(IConnectionManager connection)
        {
            //Arrange
            if (connection.GetType() != typeof(AccessOdbcConnectionManager))
                SqlTask.ExecuteNonQuery(connection, "Drop table if exists"
                   , $@"DROP TABLE IF EXISTS existtable_test");

            //Act
            var existsBefore = IfTableOrViewExistsTask.IsExisting(connection, "existtable_test");

            SqlTask.ExecuteNonQuery(connection, "Create test data table"
                , $@"CREATE TABLE existtable_test ( Col1 INT NULL )");
            var existsAfter = IfTableOrViewExistsTask.IsExisting(connection, "existtable_test");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }

        [Theory, MemberData(nameof(Connections))
            , MemberData(nameof(Access))]
        public void IfViewExists(IConnectionManager connection)
        {
            //Arrange
            if (connection.GetType() != typeof(AccessOdbcConnectionManager))
                SqlTask.ExecuteNonQuery(connection, "Drop view if exists"
                , $@"DROP VIEW IF EXISTS existview_test");

            //Act
            var existsBefore = IfTableOrViewExistsTask.IsExisting(connection, "existview_test");

            SqlTask.ExecuteNonQuery(connection, "Create test data table"
                , $@"CREATE VIEW existview_test AS SELECT 1 AS test");
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
            Assert.Throws<ETLBoxException>(
                () =>
                {
                    IfTableOrViewExistsTask.ThrowExceptionIfNotExists(connection, "xyz.Somestrangenamehere");
                });


        }
    }
}
