using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ConvertIntoObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationRowTransformation");
            CsvSource<string[]> source = new CsvSource<string[]>("res/RowTransformation/TwoColumns.csv");

            //Act
            RowTransformation<string[], MySimpleRow> trans = new RowTransformation<string[], MySimpleRow>(
                csvdata =>
                {
                    return new MySimpleRow()
                    {
                        Col1 = int.Parse(csvdata[0]),
                        Col2 = csvdata[1]
                    };
                });
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "DestinationRowTransformation");
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void InitAction()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationRowTransformation");
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/RowTransformation/TwoColumnsIdMinus1.csv");

            //Act
            int IdOffset = 0;
            RowTransformation<MySimpleRow, MySimpleRow> trans = new RowTransformation<MySimpleRow, MySimpleRow>(
                "RowTransformation testing init Action",
                row =>
                {
                    row.Col1 += IdOffset;
                    return row;
                },
                () => IdOffset += 1
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "DestinationRowTransformation");
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
