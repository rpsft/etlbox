using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.Sort
{
    [Collection("DataFlow")]
    public class SortTests
    {
        private SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SortSimpleDataDescending()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("SortSource");
            source2Columns.InsertTestData();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(Connection, "SortSource");

            //Act
            List<MySimpleRow> actual = new List<MySimpleRow>();
            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
                row => actual.Add(row)
            );
            int Comp(MySimpleRow x, MySimpleRow y) => y.Col1 - x.Col1;
            Sort<MySimpleRow> block = new Sort<MySimpleRow>(Comp);
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            List<int> expected = new List<int> { 3, 2, 1 };
            Assert.Equal(expected, actual.Select(row => row.Col1).ToList());
        }
    }
}
