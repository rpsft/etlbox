using ALE.ETLBox;
using ALE.ETLBox.Common;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using EtlBox.Database.Tests.Infrastructure;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ControlFlow
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class CreateDatabaseTaskTests : ControlFlowTestBase
    {
        protected CreateDatabaseTaskTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
        }

        [Fact]
        public void CreateSimple()
        {
            //Arrange
            var dbName = "ETLBox_" + HashHelper.RandomString(10);
            var dbListBefore = GetDatabaseListTask.List(ConnectionManager);
            Assert.DoesNotContain(dbName, dbListBefore);

            //Act
            CreateDatabaseTask.Create(ConnectionManager, dbName);

            //Assert
            var dbListAfter = GetDatabaseListTask.List(ConnectionManager);
            Assert.Contains(dbName, dbListAfter);

            //Cleanup
            DropDatabaseTask.Drop(ConnectionManager, dbName);
        }

        [Fact]
        public void CreateWithCollation()
        {
            //Arrange
            var dbName = "ETLBox_" + HashHelper.RandomString(10);
            var collation = "Latin1_General_CS_AS";
            if (ConnectionManager.GetType() == typeof(PostgresConnectionManager))
                collation = "en_US.utf8";
            if (ConnectionManager.GetType() == typeof(MySqlConnectionManager))
                collation = "latin1_swedish_ci";
            //Act
            CreateDatabaseTask.Create(ConnectionManager, dbName, collation);

            //Assert
            var dbList = GetDatabaseListTask.List(ConnectionManager);
            Assert.Contains(dbName, dbList);

            //Cleanup
            DropDatabaseTask.Drop(ConnectionManager, dbName);
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

        public class SqlServer : CreateDatabaseTaskTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) 
                : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : CreateDatabaseTaskTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }

        public class ClickHouse: CreateDatabaseTaskTests
        {
            public ClickHouse(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.ClickHouse, logger)
            {
            }
        }
    }
}
