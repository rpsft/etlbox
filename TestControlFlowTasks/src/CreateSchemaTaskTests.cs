using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateSchemaTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllConnectionsWithoutSQLite("ControlFlow");
        public CreateSchemaTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void CreateSchema(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager)
                && connection.GetType() != typeof(OracleConnectionManager)
                )
            {
                //Arrange
                string schemaName = "s" + TestHashHelper.RandomString(9);
                //Act
                CreateSchemaTask.Create(connection, schemaName);
                //Assert
                Assert.True(IfSchemaExistsTask.IsExisting(connection, schemaName));
            }
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateSchemaWithSpecialChar(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager)
                 && connection.GetType() != typeof(OracleConnectionManager)
                )
            {
                string QB = connection.QB;
                string QE = connection.QE;
                //Arrange
                string schemaName = $"{QB} s#!/ {QE}";
                //Act
                CreateSchemaTask.Create(connection, schemaName);
                //Assert
                Assert.True(IfSchemaExistsTask.IsExisting(connection, schemaName));
            }
        }
    }
}
