using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.RowTransformation
{
    public class RowTransformationStringArrayTests : TransformationsTestBase
    {
        public RowTransformationStringArrayTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void RearrangeSwappedData()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "DestinationRowTransformation"
            );
            var source = new CsvSource<string[]>(
                "res/RowTransformation/TwoColumnsSwapped.csv"
            );

            //Act
            var trans = new RowTransformation<string[]>(csvdata =>
            {
                return new[] { csvdata[1], csvdata[0] };
            });

            var dest = new DbDestination<string[]>(
                SqlConnection,
                "DestinationRowTransformation"
            );
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
