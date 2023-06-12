using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
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
            string schemaName = "s" + HashHelper.RandomString(9);
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

            string qb = connection.QB;
            string qe = connection.QE;
            //Arrange
            string schemaName = $"{qb} s#!/ {qe}";
            //Act
            CreateSchemaTask.Create(connection, schemaName);
            //Assert
            Assert.True(IfSchemaExistsTask.IsExisting(connection, schemaName));
        }
    }
}
