using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.Sort
{
    [Collection("DataFlow")]
    public class SortStringArrayTests
    {
        private SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SortSimpleDataDescending()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "SortSourceNonGeneric"
            );
            source2Columns.InsertTestData();
            DbSource<string[]> source = new DbSource<string[]>(Connection, "SortSourceNonGeneric");

            //Act
            List<string[]> actual = new List<string[]>();
            CustomDestination<string[]> dest = new CustomDestination<string[]>(
                row => actual.Add(row)
            );
            int Comp(string[] x, string[] y) => int.Parse(y[0]) - int.Parse(x[0]);
            Sort<string[]> block = new Sort<string[]>(Comp);
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            List<int> expected = new List<int> { 3, 2, 1 };
            Assert.Equal(expected, actual.Select(row => int.Parse(row[0])).ToList());
        }
    }
}
