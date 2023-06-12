using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.RowMultiplication
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
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "RowMultiplicationSource"
            );
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "RowMultiplicationSource"
            );
            RowMultiplication<MySimpleRow> multiplication =
                new RowMultiplication<MySimpleRow>(row =>
                {
                    List<MySimpleRow> result = new List<MySimpleRow> { row };
                    if (row.Col1 == 2)
                        throw new Exception("Error in Flow!");
                    return result;
                });
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

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
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "RowMultiplicationSource"
            );
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "RowMultiplicationSource"
            );
            RowMultiplication<MySimpleRow> multiplication =
                new RowMultiplication<MySimpleRow>(row =>
                {
                    List<MySimpleRow> result = new List<MySimpleRow> { row };
                    if (row.Col1 == 2)
                        throw new Exception("Error in Flow!");
                    return result;
                });
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

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
