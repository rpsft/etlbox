using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestShared.src.SharedFixtures;
using TestTransformations.src;
using TestTransformations.src.Fixtures;

namespace TestTransformations.src.RowMultiplication
{
    public class RowMultiplicationErrorLinkingTests : TransformationsTestBase
    {
        public RowMultiplicationErrorLinkingTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Serializable]
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ThrowExceptionInFlow()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                "RowMultiplicationSource"
            );
            source2Columns.InsertTestData();

            var source = new DbSource<MySimpleRow>(
                SqlConnection,
                "RowMultiplicationSource"
            );
            var multiplication =
                new RowMultiplication<MySimpleRow>(row =>
                {
                    var result = new List<MySimpleRow> { row };
                    if (row.Col1 == 2)
                        throw new Exception("Error in Flow!");
                    return result;
                });
            var dest = new MemoryDestination<MySimpleRow>();
            var errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);
            multiplication.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Collection(
                errorDest.Data,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }

        [Fact]
        public void ThrowExceptionWithoutHandling()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                "RowMultiplicationSource"
            );
            source2Columns.InsertTestData();

            var source = new DbSource<MySimpleRow>(
                SqlConnection,
                "RowMultiplicationSource"
            );
            var multiplication =
                new RowMultiplication<MySimpleRow>(row =>
                {
                    var result = new List<MySimpleRow> { row };
                    if (row.Col1 == 2)
                        throw new Exception("Error in Flow!");
                    return result;
                });
            var dest = new MemoryDestination<MySimpleRow>();

            //Act & Assert
            source.LinkTo(multiplication);
            multiplication.LinkTo(dest);

            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
