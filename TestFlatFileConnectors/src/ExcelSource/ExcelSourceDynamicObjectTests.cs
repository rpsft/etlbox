using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.ExcelSource
{
    [Collection("DataFlow")]
    public class ExcelSourceDynamicObjectTests
    {
        private SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleDataNoHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "ExcelDestinationDynamic"
            );

            //Act
            ALE.ETLBox.DataFlow.ExcelSource source = new ALE.ETLBox.DataFlow.ExcelSource(
                "res/Excel/TwoColumnShiftedData.xlsx"
            )
            {
                Range = new ExcelRange(3, 4),
                HasNoHeader = true
            };
            RowTransformation trans = new RowTransformation(row =>
            {
                dynamic r = row;
                r.Col1 = r.Column1;
                r.Col2 = r.Column2;
                return r;
            });
            DbDestination dest = new DbDestination(Connection, "ExcelDestinationDynamic");

            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void SimpleDataWithHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "ExcelDestinationDynamicWithHeader"
            );
            ALE.ETLBox.DataFlow.ExcelSource source = new ALE.ETLBox.DataFlow.ExcelSource(
                "res/Excel/TwoColumnWithHeader.xlsx"
            );
            DbDestination dest = new DbDestination(Connection, "ExcelDestinationDynamicWithHeader");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void ShiftedDataWithHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "ExcelDestinationDynamicWithHeader"
            );
            ALE.ETLBox.DataFlow.ExcelSource source = new ALE.ETLBox.DataFlow.ExcelSource(
                "res/Excel/TwoColumnShiftedDataWithHeader.xlsx"
            )
            {
                Range = new ExcelRange(3, 3)
            };
            DbDestination dest = new DbDestination(Connection, "ExcelDestinationDynamicWithHeader");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
