using ALE.ETLBox.Common;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using EtlBox.Database.Tests.Infrastructure;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ControlFlow
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class CreateSchemaTaskTests : ControlFlowTestBase
    {
        protected CreateSchemaTaskTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
        }

        [Fact]
        public void CreateSchema()
        {
            if (ConnectionManager.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            //Arrange
            var schemaName = "s" + HashHelper.RandomString(9);
            //Act
            CreateSchemaTask.Create(ConnectionManager, schemaName);
            //Assert
            Assert.True(IfSchemaExistsTask.IsExisting(ConnectionManager, schemaName));
        }

        [Fact]
        public void CreateSchemaWithSpecialChar()
        {
            if (ConnectionManager.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            var qb = ConnectionManager.QB;
            var qe = ConnectionManager.QE;
            //Arrange
            var schemaName = $"{qb} s#!/ {qe}";
            //Act
            CreateSchemaTask.Create(ConnectionManager, schemaName);
            //Assert
            Assert.True(IfSchemaExistsTask.IsExisting(ConnectionManager, schemaName));
        }

        public class SqlServer : CreateSchemaTaskTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : CreateSchemaTaskTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
