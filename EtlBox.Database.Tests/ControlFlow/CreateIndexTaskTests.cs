using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using EtlBox.Database.Tests.Infrastructure;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ControlFlow
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class CreateIndexTaskTests : ControlFlowTestBase
    {
        protected CreateIndexTaskTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
        }

        [Fact]
        public void CreateIndex()
        {
            //Arrange
            const string indexName = "ix_IndexTest1";
            CreateTableTask.Create(
                ConnectionManager,
                "IndexCreationTable1",
                new List<TableColumn>
                {
                    new("Key1", "INT", allowNulls: false),
                    new("Key2", "INT", allowNulls: true)
                }
            );

            //Act
            CreateIndexTask.CreateOrRecreate(
                ConnectionManager,
                indexName,
                "IndexCreationTable1",
                new List<string> { "Key1", "Key2" }
            );

            //Assert
            Assert.True(
                IfIndexExistsTask.IsExisting(ConnectionManager, "ix_IndexTest1", "IndexCreationTable1")
            );
        }

        [Fact]
        public void ReCreateIndex()
        {
            //Arrange
            const string indexName = "ix_IndexTest2";
            CreateTableTask.Create(
                 ConnectionManager,
                "IndexCreationTable2",
                new List<TableColumn>
                {
                    new("Key1", "INT", allowNulls: false),
                    new("Key2", "INT", allowNulls: true)
                }
            );
            CreateIndexTask.CreateOrRecreate(
                ConnectionManager,
                indexName,
                "IndexCreationTable2",
                new List<string> { "Key1", "Key2" }
            );

            //Act
            CreateIndexTask.CreateOrRecreate(
                ConnectionManager,
                indexName,
                "IndexCreationTable2",
                new List<string> { "Key1", "Key2" }
            );

            //Assert
            Assert.True(
                IfIndexExistsTask.IsExisting(ConnectionManager, "ix_IndexTest2", "IndexCreationTable2")
            );
        }

        [Fact]
        public void CreateIndexWithInclude()
        {
            //Arrange
            const string indexName = "ix_IndexTest3";
            CreateTableTask.Create(
                ConnectionManager,
                "IndexCreation3",
                new List<TableColumn>
                {
                    new("Key1", "INT", allowNulls: false),
                    new("Key2", "CHAR(2)", allowNulls: true),
                    new("Value1", "NVARCHAR(10)", allowNulls: true),
                    new("Value2", "DECIMAL(10,2)", allowNulls: false)
                }
            );
            //Act
            CreateIndexTask.CreateOrRecreate(
                ConnectionManager,
                indexName,
                "IndexCreation3",
                new List<string> { "Key1", "Key2" },
                new List<string> { "Value1", "Value2" }
            );
            //Assert
            Assert.True(
                IfIndexExistsTask.IsExisting(ConnectionManager, "ix_IndexTest3", "IndexCreation3")
            );
        }

        public class SqlServer : CreateIndexTaskTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : CreateIndexTaskTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }

        public class ClickHouse : CreateIndexTaskTests
        {
            public ClickHouse(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
