using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
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
