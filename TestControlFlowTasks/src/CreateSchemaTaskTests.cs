using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Helper;
using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class CreateSchemaTaskTests : ControlFlowTestBase
    {
        public CreateSchemaTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLite))]
        public void CreateSchema(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            //Arrange
            var schemaName = "s" + HashHelper.RandomString(9);
            //Act
            CreateSchemaTask.Create(connection, schemaName);
            //Assert
            Assert.True(IfSchemaExistsTask.IsExisting(connection, schemaName));
        }

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLite))]
        public void CreateSchemaWithSpecialChar(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            var qb = connection.QB;
            var qe = connection.QE;
            //Arrange
            var schemaName = $"{qb} s#!/ {qe}";
            //Act
            CreateSchemaTask.Create(connection, schemaName);
            //Assert
            Assert.True(IfSchemaExistsTask.IsExisting(connection, schemaName));
        }
    }
}
