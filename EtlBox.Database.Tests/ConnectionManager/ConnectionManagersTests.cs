using System.Dynamic;
using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using ETLBox.Primitives;
using FluentAssertions;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ConnectionManager
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class ConnectionManagersTests : DatabaseTestBase
    {
        protected ConnectionManagersTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
        }

        [Fact]
        public void SimpleDataFlowTest()
        {
            // Arrange
            var manager = _fixture.GetContainer(_connectionType).GetConnectionManager();
            var table = new TableDefinition
            {
                Name = "Test",
                Columns = new List<TableColumn>()
                {
                    new TableColumn()
                    {
                        Name = "Col1",
                        DataType = _connectionType switch
                        {
                            ConnectionManagerType.ClickHouse => "String",
                            _ => "varchar(32)"
                        },
                        IsPrimaryKey = true,
                    },
                    new TableColumn()
                    {
                        Name = "Col2",
                        DataType = _connectionType switch
                        {
                            ConnectionManagerType.Postgres => "timestamp",
                            _ => "DateTime",
                        }
                    }
                }
            };
            CreateTableTask.Create(manager, table);

            //Act
            manager.Open();
            var res = manager.ExecuteScalar($"select count(*) from {QB}{table.Name}{QE}");

            //Assert
            res.Should().Be(0);

            dynamic obj = new ExpandoObject();
            obj.Col1 = "TestValue";
            obj.Col2 = DateTime.Now;

            var source = new MemorySource<ExpandoObject>(new[] { (ExpandoObject)obj });

            var dest = new DbDestination<ExpandoObject>(manager, table.Name);

            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            res = manager.ExecuteScalar($"select count(*) from {QB}{table.Name}{QE}");
            res.Should().Be(1);
        }

        public class ClickHouse : ConnectionManagersTests
        {
            public ClickHouse(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.ClickHouse, logger)
            {
            }
        }

        public class SqlServer : ConnectionManagersTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : ConnectionManagersTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
