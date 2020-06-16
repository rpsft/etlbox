using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class IfIndexExistsTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public IfIndexExistsTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void IfIndexExists(IConnectionManager connection)
        {
            //Arrange
            SqlTask.ExecuteNonQuery(connection, "Create index test table"
               , $@"CREATE TABLE {connection.QB}indextable{connection.QE} (col1 INT NULL)");

            //Act
            var existsBefore = IfIndexExistsTask.IsExisting(connection, "index_test", "indextable");
            SqlTask.ExecuteNonQuery(connection, "Create test index"
                , $@"CREATE INDEX {connection.QB}index_test{connection.QE} ON {connection.QB}indextable{connection.QE} (col1)");

            var existsAfter = IfIndexExistsTask.IsExisting(connection, "index_test", "indextable");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }


    }
}
