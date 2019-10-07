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
    public class IfProcedureExistsTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllConnectionsWithoutSQLite("ControlFlow");

        public IfProcedureExistsTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

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
