using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ExcelSourceDynamicObjectTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public ExcelSourceDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleDataNoHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestinationDynamic");

            //Act
            ExcelSource source = new ExcelSource("res/Excel/TwoColumnShiftedData.xlsx")
            {
                Range = new ExcelRange(3, 4),
                HasNoHeader = true
            };
            RowTransformation trans = new RowTransformation(row =>
            {
                dynamic r = row as dynamic;
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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestinationDynamicWithHeader");
            ExcelSource source = new ExcelSource("res/Excel/TwoColumnWithHeader.xlsx");
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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestinationDynamicWithHeader");
            ExcelSource source = new ExcelSource("res/Excel/TwoColumnShiftedDataWithHeader.xlsx");
            source.Range = new ExcelRange(3, 3);
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
