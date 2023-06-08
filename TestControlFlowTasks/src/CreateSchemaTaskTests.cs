using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class CreateSchemaTaskTests
    {
        public static IEnumerable<object[]> Connections =>
            Config.AllConnectionsWithoutSQLite("ControlFlow");

        [Theory, MemberData(nameof(Connections))]
        public void CreateSchema(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager))
            {
                //Arrange
                string schemaName = "s" + HashHelper.RandomString(9);
                //Act
                CreateSchemaTask.Create(connection, schemaName);
                //Assert
                Assert.True(IfSchemaExistsTask.IsExisting(connection, schemaName));
            }
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateSchemaWithSpecialChar(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager))
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
