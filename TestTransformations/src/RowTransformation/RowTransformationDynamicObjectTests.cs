using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using TestHelper.Models;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.RowTransformation
{
    [Collection("Transformations")]
    public class RowTransformationDynamicObjectTests : TransformationsTestBase
    {
        public RowTransformationDynamicObjectTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void ConvertIntoObject()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture("DestinationRowTransformationDynamic");
            var source = new CsvSource<ExpandoObject>("res/RowTransformation/TwoColumns.csv");

            //Act
            var trans = new RowTransformation<ExpandoObject>(csvdata =>
            {
                dynamic c = csvdata;
                c.Col1 = c.Header1;
                c.Col2 = c.Header2;
                return c;
            });
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "DestinationRowTransformationDynamic"
            );
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void DestinationJsonTransformationTest()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture("DestinationJsonTransformation");
            var objSet = new ExpandoObject[]
            {
                CreateObject(@"{ ""Data"": { ""Id"": 1, ""Name"": ""Test1"" } }"),
                CreateObject(@"{ ""Data"": { ""Id"": 2, ""Name"": ""Test2"" } }"),
                CreateObject(@"{ ""Data"": { ""Id"": 3, ""Name"": ""Test3"" } }"),
            };

            var source = new MemorySource<ExpandoObject>(objSet);

            //Act
            var trans = new JsonTransformation()
            {
                Mappings = new JsonMapping[]
                {
                    new JsonMapping
                    {
                        Source = new JsonProperty { Name = "data", Path = "$.Data.Id" },
                        Destination = "Col1"
                    },
                    new JsonMapping
                    {
                        Source = new JsonProperty { Name = "data", Path = "$.Data.Name" },
                        Destination = "Col2"
                    },
                }
            };

            var dest = new DbDestination<ExpandoObject>(SqlConnection, dest2Columns.TableName);
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        private ExpandoObject CreateObject(string v)
        {
            dynamic obj = new ExpandoObject();
            obj.data = v;
            return obj as ExpandoObject;
        }
    }
}
