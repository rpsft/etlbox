using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class IfSchemaExistsTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllConnectionsWithoutSQLite("ControlFlow");

        public IfSchemaExistsTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void IfSchemaeExists(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager))
            {
                //Arrange
                var existsBefore = IfSchemaExistsTask.IsExisting(connection, "testschema");
                CreateSchemaTask.Create(connection, "testschema");

                //Act
                var existsAfter = IfSchemaExistsTask.IsExisting(connection, "testschema");

                //Assert
                Assert.False(existsBefore);
                Assert.True(existsAfter);
            }
        }
    }
}
