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
