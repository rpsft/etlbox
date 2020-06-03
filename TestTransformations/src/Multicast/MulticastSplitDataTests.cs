using ETLBox.Connection;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MulticastSplitDataTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MulticastSplitDataTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        internal class CSVPoco
        {
            public int CSVCol1 { get; set; }
            public string CSVCol2 { get; set; }
            public long? CSVCol3 { get; set; }
            public decimal CSVCol4 { get; set; }
        }

        internal class Entity1
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        internal class Entity2
        {
            public string Col2 { get; set; }
            public long? Col3 { get; set; }
            public decimal Col4 { get; set; }
        }

        [Fact]
        public void SplitCsvSourceIn2Tables()
        {
            //Arrange
            TwoColumnsTableFixture dest1Table = new TwoColumnsTableFixture("SplitDataDestination1");
            FourColumnsTableFixture dest2Table = new FourColumnsTableFixture("SplitDataDestination2");

            var source = new CsvSource<CSVPoco>("res/Multicast/CsvSourceToSplit.csv");
            source.Configuration.Delimiter = ";";

            var multicast = new Multicast<CSVPoco>();

            var row1 = new RowTransformation<CSVPoco, Entity1>(input =>
            {
                return new Entity1
                {
                    Col1 = input.CSVCol1,
                    Col2 = input.CSVCol2
                };
            });
            var row2 = new RowTransformation<CSVPoco, Entity2>(input =>
            {
                return new Entity2
                {
                    Col2 = input.CSVCol2,
                    Col3 = input.CSVCol3,
                    Col4 = input.CSVCol4
                };
            });

            var destination1 = new DbDestination<Entity1>(Connection, "SplitDataDestination1");
            var destination2 = new DbDestination<Entity2>(Connection, "SplitDataDestination2");

            //Act
            source.LinkTo(multicast);
            multicast.LinkTo(row1);
            multicast.LinkTo(row2);

            row1.LinkTo(destination1);
            row2.LinkTo(destination2);

            source.Execute();
            destination1.Wait();
            destination2.Wait();

            //Assert
            dest1Table.AssertTestData();
            dest2Table.AssertTestData();
        }
    }
}
