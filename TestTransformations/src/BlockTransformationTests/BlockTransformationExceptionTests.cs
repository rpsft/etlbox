using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.BlockTransformationTests
{
    public class BlockTransformationExceptionTests : TransformationsTestBase
    {
        public BlockTransformationExceptionTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

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
                SqlConnection,
                "BlockTransSource"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "BlockTransDest"
            );

            //Act
            BlockTransformation<MySimpleRow> block = new BlockTransformation<MySimpleRow>(
                _ => throw new Exception("Test")
            );
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
