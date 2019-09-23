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
    public class IfExistsTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public IfExistsTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void IfTableExists(IConnectionManager connection)
        {
            //Arrange
            SqlTask.ExecuteNonQuery(connection,"Drop table if exists"
               , $@"DROP TABLE IF EXISTS exist_test");

            //Act
            var existsBefore = IfTableExistsTask.IsExisting(connection, "exist_test");

            SqlTask.ExecuteNonQuery(connection, "Create test data table"
                , $@"CREATE TABLE exist_test ( Col1 INT NULL )");
            var existsAfter = IfTableExistsTask.IsExisting(connection, "exist_test");

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
                    IfTableExistsTask.ThrowExceptionIfNotExists(connection, "xyz.Somestrangenamehere");
                });


        }
    }
}
