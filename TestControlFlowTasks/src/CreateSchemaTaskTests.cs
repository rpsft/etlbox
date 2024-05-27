using ALE.ETLBox.Common;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    [Collection(nameof(ControlFlowCollection))]
    public class CreateSchemaTaskTests : ControlFlowTestBase
    {
        public CreateSchemaTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLiteAndClickHouse))]
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

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLiteAndClickHouse))]
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
