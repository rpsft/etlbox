using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.CustomSource
{
    public class CustomSourceStringArrayTests : OtherConnectorsTestBase
    {
        public CustomSourceStringArrayTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "Destination4CustomSourceNonGeneric"
            );
            var data = new List<string> { "Test1", "Test2", "Test3" };
            var readIndex = 0;

            string[] ReadData()
            {
                var result = new string[2];
                result[0] = (readIndex + 1).ToString();
                result[1] = data[readIndex];
                readIndex++;
                return result;
            }

            bool EndOfData() => readIndex >= data.Count;

            //Act
            var source = new CustomSource<string[]>(ReadData, EndOfData);
            var dest = new DbDestination<string[]>(
                SqlConnection,
                "Destination4CustomSourceNonGeneric"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
