using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationStringArrayTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void RearrangeSwappedData()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationRowTransformation");
            CsvSource<string[]> source = new CsvSource<string[]>("res/RowTransformation/TwoColumnsSwapped.csv");

            //Act
            RowTransformation<string[]> trans = new RowTransformation<string[]>(
                csvdata =>
                {
                    return new string[] { csvdata[1], csvdata[0] };
                });

            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "DestinationRowTransformation");
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }


    }
}
