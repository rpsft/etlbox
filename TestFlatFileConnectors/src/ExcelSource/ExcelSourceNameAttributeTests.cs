using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.ExcelSource
{
    [Collection("DataFlow")]
    public class ExcelSourceNameAttributeTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            [ExcelColumn("Col1")]
            [ColumnMap("Col1")]
            public int Column1 { get; set; }

            [ExcelColumn("Col2")]
            [ColumnMap("Col2")]
            public string Column2 { get; set; }
            public string ExtraColumn { get; set; }
        }

        [Fact]
        public void SimpleData()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "ExcelDestinationWithNameAttribute"
            );
            ExcelSource<MySimpleRow> source = new ExcelSource<MySimpleRow>(
                "res/Excel/TwoColumnWithHeader.xlsx"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                Connection,
                "ExcelDestinationWithNameAttribute"
            );

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
