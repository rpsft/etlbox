using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.CustomSource
{
    public class CustomSourceErrorLinkingTests : OtherConnectorsTestBase
    {
        public CustomSourceErrorLinkingTests(OtherConnectorsDatabaseFixture fixture)
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
                "ErrorLinkingCustomSource"
            );
            var data = new List<string> { "Test1", "Test2", "Test3", "Test4" };
            var readIndex = 0;

            MySimpleRow ReadData()
            {
                var result = new MySimpleRow { Col1 = readIndex + 1, Col2 = data[readIndex] };
                readIndex++;
                if (readIndex == 4)
                    throw new Exception("Error record!");
                return result;
            }

            bool EndOfData() => readIndex >= data.Count;

            var source = new CustomSource<MySimpleRow>(ReadData, EndOfData);
            var dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "ErrorLinkingCustomSource"
            );
            var errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection(
                errorDest.Data,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }
    }
}
