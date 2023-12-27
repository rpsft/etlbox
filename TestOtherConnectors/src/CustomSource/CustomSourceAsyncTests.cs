using System.Threading;
using ALE.ETLBox.Common;
using ALE.ETLBox.ControlFlow;
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
        public void SimpleAsyncFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "Destination4CustomSource"
            );
            List<string> data = new List<string> { "Test1", "Test2", "Test3" };
            int readIndex = 0;

            MySimpleRow ReadData()
            {
                if (readIndex == 0)
                    Task.Delay(300).Wait();
                var result = new MySimpleRow { Col1 = readIndex + 1, Col2 = data[readIndex] };
                readIndex++;
                return result;
            }

            bool EndOfData() => readIndex >= data.Count;

            //Act
            CustomSource<MySimpleRow> source = new CustomSource<MySimpleRow>(ReadData, EndOfData);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "Destination4CustomSource"
            );
            source.LinkTo(dest);
            Task sourceT = source.ExecuteAsync(CancellationToken.None);
            Task destT = dest.Completion;

            //Assert
            Assert.True(RowCountTask.Count(SqlConnection, "Destination4CustomSource") == 0);
            sourceT.Wait();
            destT.Wait();
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void ExceptionalAsyncFlow()
        {
            //Arrange
            ALE.ETLBox.DataFlow.CustomSource source = new ALE.ETLBox.DataFlow.CustomSource(
                () => throw new ETLBoxException("Test Exception"),
                () => false
            );
            ALE.ETLBox.DataFlow.CustomDestination dest =
                new ALE.ETLBox.DataFlow.CustomDestination(_ => { });

            //Act
            source.LinkTo(dest);

            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                Task sourceT = source.ExecuteAsync(CancellationToken.None);
                Task destT = dest.Completion;
                try
                {
                    sourceT.Wait();
                    destT.Wait();
                }
                catch (Exception e)
                {
                    throw e.InnerException!;
                }
            });
        }
    }
}
