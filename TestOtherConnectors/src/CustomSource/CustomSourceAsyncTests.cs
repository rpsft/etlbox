using System.Threading;
using ALE.ETLBox.Common;
using TestShared.SharedFixtures;

namespace TestOtherConnectors.CustomSource
{
    [Collection("OtherConnectors")]
    public class CustomSourceAsyncTests : OtherConnectorsTestBase
    {
        public CustomSourceAsyncTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public async Task SimpleAsyncFlow()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture("Destination4CustomSource");
            var data = new List<string> { "Test1", "Test2", "Test3" };
            var readIndex = 0;

            //Act
            var source = new CustomSource<MySimpleRow>(
                () => ReadData(ref readIndex, data),
                () => readIndex >= data.Count
            );
            var dest = new DbDestination<MySimpleRow>(SqlConnection, "Destination4CustomSource");
            source.LinkTo(dest);
            await source.ExecuteAsync(CancellationToken.None);
            await dest.Completion.ConfigureAwait(true);

            //Assert
            dest2Columns.AssertTestData();
        }

        private static MySimpleRow ReadData(ref int index, IReadOnlyList<string> data)
        {
            if (index == 0)
                Task.Delay(300).Wait();
            var result = new MySimpleRow { Col1 = index + 1, Col2 = data[index] };
            index++;
            return result;
        }

        [Fact]
        public async Task ExceptionalAsyncFlow()
        {
            //Arrange
            var source = new ALE.ETLBox.DataFlow.CustomSource(
                () => throw new ETLBoxException("Test Exception"),
                () => false
            );
            var dest = new ALE.ETLBox.DataFlow.CustomDestination(_ => { });

            //Act
            source.LinkTo(dest);

            //Assert
            await Assert.ThrowsAsync<ETLBoxException>(async () =>
            {
                try
                {
                    await source.ExecuteAsync(CancellationToken.None).ConfigureAwait(false);
                    await dest.Completion.ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    throw e.InnerException ?? e;
                }
            });
        }
    }
}
