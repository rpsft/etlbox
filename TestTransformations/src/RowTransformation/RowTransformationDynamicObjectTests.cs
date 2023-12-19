using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.RowTransformation
{
    public class RowTransformationDynamicObjectTests : TransformationsTestBase
    {
        public RowTransformationDynamicObjectTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void ConvertIntoObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "DestinationRowTransformationDynamic"
            );
            CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>(
                "res/RowTransformation/TwoColumns.csv"
            );

            //Act
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(csvdata =>
            {
                dynamic c = csvdata;
                c.Col1 = c.Header1;
                c.Col2 = c.Header2;
                return c;
            });
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
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
    }
}
