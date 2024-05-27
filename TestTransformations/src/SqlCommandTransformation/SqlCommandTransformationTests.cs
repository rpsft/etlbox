using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.src.SqlCommandTransformation
{
    public class SqlCommandTransformationTests : TransformationsTestBase
    {
        public SqlCommandTransformationTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void ConvertIntoObject()
        {
            //Arrange
            _ = new TwoColumnsTableFixture(
                "DestinationRowTransformation"
            );

            var rowsCountBefore = RowCountTask.Count(SqlConnection, "DestinationRowTransformation");

            dynamic obj = new ExpandoObject();
            obj.Col1 = 123;
            obj.Col2 = "abc";

            var settings = new MemorySource<ExpandoObject>(new[] { (ExpandoObject)obj });

            var query = new ALE.ETLBox.src.Toolbox.DataFlow.SqlCommandTransformation
            {
                ConnectionManager = SqlConnection,
                SQLTemplate = "Insert INTO DestinationRowTransformation VALUES({{Col1}}, '{{Col2}}')"
            };

            var dest = new MemoryDestination<ExpandoObject>();

            //Act
            settings.LinkTo(query);
            query.LinkTo(dest);
            settings.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(rowsCountBefore + 1, RowCountTask.Count(SqlConnection, "DestinationRowTransformation"));
        }
    }
}
