using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.BlockTransformationTests
{
    [Collection("DataFlow")]
    public class BlockTransformationExceptionTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ThrowExceptionWithoutHandling()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("BlockTransSource");
            source2Columns.InsertTestData();
            var _ = new TwoColumnsTableFixture("BlockTransDest");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                Connection,
                "BlockTransSource"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                Connection,
                "BlockTransDest"
            );

            //Act
            BlockTransformation<MySimpleRow> block = new BlockTransformation<MySimpleRow>(_ =>
            {
                throw new Exception("Test");
            });
            source.LinkTo(block);
            block.LinkTo(dest);

            //Assert
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
