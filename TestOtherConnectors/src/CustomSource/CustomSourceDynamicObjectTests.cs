using System.Dynamic;
using ALE.ETLBox.DataFlow;
using TestOtherConnectors.Fixture;
using TestShared.SharedFixtures;

namespace TestOtherConnectors.CustomSource
{
    public class CustomSourceDynamicObjectTests : OtherConnectorsTestBase
    {
        public CustomSourceDynamicObjectTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "Destination4CustomSourceDynamic"
            );
            var data = new List<string> { "Test1", "Test2", "Test3" };
            var readIndex = 0;

            ExpandoObject ReadData()
            {
                dynamic result = new ExpandoObject();
                result.Col1 = (readIndex + 1).ToString();
                result.Col2 = data[readIndex];
                readIndex++;
                return result;
            }

            bool EndOfData() => readIndex >= data.Count;

            //Act
            var source = new CustomSource<ExpandoObject>(
                ReadData,
                EndOfData
            );
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "Destination4CustomSourceDynamic"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
