using ALE.ETLBox.DataFlow;
using FluentAssertions;
using TestTransformations.Fixtures;

namespace TestTransformations.SqlQueryTransformation
{
    [Collection("Transformations")]
    public class SqlQueryTransformationTests : TransformationsTestBase
    {
        public SqlQueryTransformationTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void ShouldProcessSqlParameterWithGenericsTest()
        {
            //Arrange
            var obj = new TestSourceRecord { LastId = 123 };

            var settings = new MemorySource<TestSourceRecord>(new[] { obj });

            var query = new SqlQueryTransformation<TestSourceRecord, ExpandoObject>
            {
                ConnectionManager = SqlConnection,
                SqlTemplate = "select {{lastId}} as LastId"
            };

            var dest = new MemoryDestination<ExpandoObject>();

            //Act
            settings.LinkTo(query);
            query.LinkTo(dest);
            settings.Execute();
            dest.Wait();

            //Assert

            dest.Data.Should().HaveCount(1);
            var data = dest.Data.First() as IDictionary<string, object>;
            data["LastId"].Should().Be(obj.LastId);
        }

        private record TestSourceRecord
        {
            public int LastId { get; init; }
        }

        [Fact]
        public void ShouldProcessSqlParameterWithExpandoObjectTest()
        {
            //Arrange
            dynamic obj = new ExpandoObject();
            obj.LastId = 123;

            var settings = new MemorySource<ExpandoObject>(new[] { (ExpandoObject)obj });

            var query = new ALE.ETLBox.DataFlow.SqlQueryTransformation
            {
                ConnectionManager = SqlConnection,
                SqlTemplate = "select {{lastId}} as LastId"
            };

            var dest = new MemoryDestination<ExpandoObject>();

            //Act
            settings.LinkTo(query);
            query.LinkTo(dest);
            settings.Execute();
            dest.Wait();

            //Assert

            dest.Data.Should().HaveCount(1);
            var data = dest.Data.First() as IDictionary<string, object>;
            data["LastId"].Should().Be(obj.LastId);
        }

        [Fact]
        public void ShouldProcessSqlQueryWithCteTest()
        {
            //Arrange
            dynamic obj = new ExpandoObject();
            obj.value = "aaa";

            var settings = new MemorySource<ExpandoObject>(new[] { (ExpandoObject)obj });

            var query = new ALE.ETLBox.DataFlow.SqlQueryTransformation
            {
                ConnectionManager = SqlConnection,
                SqlTemplate =
                    @"
                with data as (
                    select 1 as id, '{{value}}' as value
                )
                select * from data",
                SourceTableDefinition = new ALE.ETLBox.TableDefinition(
                    "source",
                    new List<ALE.ETLBox.TableColumn>
                    {
                        new("id", "int"),
                        new("value", "varchar(16)")
                    }
                )
            };

            var dest = new MemoryDestination<ExpandoObject>();

            //Act
            settings.LinkTo(query);
            query.LinkTo(dest);
            settings.Execute();
            dest.Wait();

            //Assert

            dest.Data.Should().HaveCount(1);
            var data = dest.Data.First() as IDictionary<string, object>;
            data["id"].Should().Be(1);
            data["value"].Should().Be(obj.value);
        }
    }
}
