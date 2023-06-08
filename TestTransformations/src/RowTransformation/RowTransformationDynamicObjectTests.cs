using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.RowTransformation
{
    [Collection("DataFlow")]
    public class RowTransformationDynamicObjectTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

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
                Connection,
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
