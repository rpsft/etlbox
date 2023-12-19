using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using EtlBox.Database.Tests.Infrastructure;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ControlFlow
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class CreateProcedureTaskTests : ControlFlowTestBase
    {
        protected CreateProcedureTaskTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
        }

        [Fact]
        public void CreateProcedure()
        {
            //Arrange
            //Act
            CreateProcedureTask.CreateOrAlter(ConnectionManager, "Proc1", "SELECT 1;");
            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(ConnectionManager, "Proc1"));
        }

        [Fact]
        public void AlterProcedure()
        {
            //Arrange
            CreateProcedureTask.CreateOrAlter(ConnectionManager, "Proc2", "SELECT 1;");
            IfProcedureExistsTask.IsExisting(ConnectionManager, "Proc2");

            //Act
            CreateProcedureTask.CreateOrAlter(ConnectionManager, "Proc2", "SELECT 5;");

            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(ConnectionManager, "Proc2"));
        }

        [Fact]
        public void CreateProcedureWithParameter()
        {
            //Arrange
            var pars = new List<ProcedureParameter>
            {
                new("Par1", "VARCHAR(10)"),
                new("Par2", "INT", "7")
            };
            //Act
            CreateProcedureTask.CreateOrAlter(ConnectionManager, "Proc3", "SELECT 1;", pars);
            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(ConnectionManager, "Proc3"));
        }

        [Fact]
        public void CreateProcedureWithProcedureObject()
        {
            //Arrange
            var pars = new List<ProcedureParameter>
            {
                new("Par1", "varchar(10)"),
                new("Par2", "int", "7")
            };
            var procDef = new ProcedureDefinition("Proc4", "SELECT 1;", pars);
            //Act
            CreateProcedureTask.CreateOrAlter(ConnectionManager, procDef);
            //Assert
            Assert.True(IfProcedureExistsTask.IsExisting(ConnectionManager, "Proc4"));
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            if (ConnectionManager.ConnectionManagerType == ConnectionManagerType.SQLite)
            {
                Assert.Throws<ETLBoxNotSupportedException>(
                    () => CreateDatabaseTask.Create(ConnectionManager, "Test")
                );
            }
        }

        public class SqlServer : CreateProcedureTaskTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : CreateProcedureTaskTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
