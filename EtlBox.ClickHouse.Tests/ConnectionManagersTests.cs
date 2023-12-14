using System.Dynamic;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using FluentAssertions;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests
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
        public void Test1()
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
                        DataType = "String",
                    },
                    new TableColumn()
                    {
                        Name = "Col2",
                        DataType = "DateTime",
                    }
                },
                Engine = "MergeTree()",
                OrderBy = "Col1"
            };
            CreateTableTask.Create(manager, table);

            //Act
            manager.Open();
            var res = (ulong)manager.ExecuteScalar($"select count(*) from `{table.Name}`");

            //Assert
            res.Should().Be(0);

            dynamic obj = new ExpandoObject();
            obj.Col1 = "TestValue";
            obj.Col2 = DateTime.Now;

            var source = new MemorySource<ExpandoObject>(new[] { (ExpandoObject)obj });

            var dest = new DbDestination<ExpandoObject>(manager, table.Name);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            res = (ulong)manager.ExecuteScalar($"select count(*) from `{table.Name}`");
            res.Should().Be(1);
        }

        public class ClickHouse : ConnectionManagersTests
        {
            public ClickHouse(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.ClickHouse, logger)
            {
            }
        }
    }
}
