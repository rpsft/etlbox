using ALE.ETLBox.DataFlow;
using ALE.ETLBox.src.Toolbox.DataFlow;
using FluentAssertions;
using TestTransformations.Fixtures;

namespace TestTransformations.RowMultiplication
{
    [Collection("Transformations")]
    public class SqlQueryTransformationTests : TransformationsTestBase
    {
        public SqlQueryTransformationTests(TransformationsDatabaseFixture fixture) : 
            base(fixture) { }

        [Fact]
        public void ShouldProcessSqlParameterTest()
        {
            //Arrange               
            dynamic obj = new ExpandoObject();
            obj.LastId = 123;

            var settings = new MemorySource<ExpandoObject>(new []{ (ExpandoObject)obj });

            var query = new SqlQueryTransformation();
            query.ConnectionManager = SqlConnection;
            query.SQLTemplate = "select {{lastId}} as LastId";

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
        public void ShouldProcessSqlQueryWithCTETest()
        {
            //Arrange               
            dynamic obj = new ExpandoObject();
            obj.value = "aaa";

            var settings = new MemorySource<ExpandoObject>(new []{ (ExpandoObject)obj });

            var query = new SqlQueryTransformation();
            query.ConnectionManager = SqlConnection;
            query.SQLTemplate = @"
                with data as (
                    select 1 as id, '{{value}}' as value
                )
                select * from data";
            query.SourceTableDefinition = new ALE.ETLBox.TableDefinition(
                "source",
                new List<ALE.ETLBox.TableColumn>
                { 
                    new ALE.ETLBox.TableColumn("id", "int"),
                    new ALE.ETLBox.TableColumn("value", "varchar(16)")
                });

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
