using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
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
            SqlTask.ExecuteNonQuery(connection,"Create index test table"
               , $@"CREATE TABLE indextable (col1 INT NULL)");

            //Act
            var existsBefore = IfIndexExistsTask.IsExisting(connection, "index_test", "indextable");

            SqlTask.ExecuteNonQuery(connection, "Create test index"
                , $@"CREATE INDEX index_test ON indextable (col1)");
            var existsAfter = IfIndexExistsTask.IsExisting(connection, "index_test", "indextable");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }


    }
}
