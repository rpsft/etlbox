using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class CreateProcedureTaskTests : ControlFlowTestBase
    {
        public CreateProcedureTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLite))]
        public void CreateProcedure(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateProcedureTask.CreateOrAlter(connection, "Proc1", "SELECT 1;");
            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(connection, "Proc1"));
        }

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLite))]
        public void AlterProcedure(IConnectionManager connection)
        {
            //Arrange
            CreateProcedureTask.CreateOrAlter(connection, "Proc2", "SELECT 1;");
            IfProcedureExistsTask.IsExisting(connection, "Proc2");

            //Act
            CreateProcedureTask.CreateOrAlter(connection, "Proc2", "SELECT 5;");

            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(connection, "Proc2"));
        }

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLite))]
        public void CreateProcedureWithParameter(IConnectionManager connection)
        {
            //Arrange
            var pars = new List<ProcedureParameter>
            {
                new("Par1", "VARCHAR(10)"),
                new("Par2", "INT", "7")
            };
            //Act
            CreateProcedureTask.CreateOrAlter(connection, "Proc3", "SELECT 1;", pars);
            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(connection, "Proc3"));
        }

        [Theory, MemberData(nameof(AllConnectionsWithoutSQLite))]
        public void CreateProcedureWithProcedureObject(IConnectionManager connection)
        {
            //Arrange
            var pars = new List<ProcedureParameter>
            {
                new("Par1", "varchar(10)"),
                new("Par2", "int", "7")
            };
            var procDef = new ProcedureDefinition("Proc4", "SELECT 1;", pars);
            //Act
            CreateProcedureTask.CreateOrAlter(connection, procDef);
            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(connection, "Proc4"));
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => CreateDatabaseTask.Create(SqliteConnection, "Test")
            );
        }
    }
}
