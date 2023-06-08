using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.ExcelSource
{
    [Collection("DataFlow")]
    public class ExcelSourceStringArrayTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MyData
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleDataNoHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "ExcelDestinationStringArray"
            );

            //Act
            ExcelSource<string[]> source = new ExcelSource<string[]>("res/Excel/TwoColumnData.xlsx")
            {
                HasNoHeader = true
            };
            RowTransformation<string[], MyData> trans = new RowTransformation<
                string[],
                MyData
            >(row =>
            {
                MyData result = new MyData { Col1 = int.Parse(row[0]), Col2 = row[1] };
                return result;
            });
            DbDestination<MyData> dest = new DbDestination<MyData>(
                Connection,
                "ExcelDestinationStringArray"
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
