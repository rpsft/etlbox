using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBDestination
{
    [Collection("DataFlow")]
    public class DbDestinationNullHandlingTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(
                SqlConnection,
                "DestIgnoreNullValues"
            );
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>
            {
                DataAsList = new List<MySimpleRow>
                {
                    null,
                    new MySimpleRow { Col1 = 1, Col2 = "Test1" },
                    null,
                    new MySimpleRow { Col1 = 2, Col2 = "Test2" },
                    new MySimpleRow { Col1 = 3, Col2 = "Test3" },
                    null
                }
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "DestIgnoreNullValues"
            );

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

        [Fact]
        public void IgnoreWithStringArray()
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(
                SqlConnection,
                "DestIgnoreNullValuesStringArray"
            );
            MemorySource<string[]> source = new MemorySource<string[]>
            {
                DataAsList = new List<string[]>
                {
                    null,
                    new[] { "1", "Test1" },
                    null,
                    new[] { "2", "Test2" },
                    new[] { "3", "Test3" },
                    null
                }
            };
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "DestIgnoreNullValuesStringArray"
            );

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }
    }
}
