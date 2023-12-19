using ALE.ETLBox.DataFlow;
using TestOtherConnectors.Fixture;
using TestShared.SharedFixtures;

namespace TestOtherConnectors.CustomSource
{
    public class CustomSourceTests : OtherConnectorsTestBase
    {
        public CustomSourceTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Serializable]
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "Destination4CustomSource"
            );
            var data = new List<string> { "Test1", "Test2", "Test3" };
            var readIndex = 0;

            MySimpleRow ReadData()
            {
                var result = new MySimpleRow { Col1 = readIndex + 1, Col2 = data[readIndex] };
                readIndex++;
                return result;
            }

            bool EndOfData() => readIndex >= data.Count;

            //Act
            var source = new CustomSource<MySimpleRow>(ReadData, EndOfData);
            var dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "Destination4CustomSource"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
