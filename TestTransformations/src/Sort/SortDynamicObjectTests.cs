using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.Sort
{
    [Collection("DataFlow")]
    public class SortDynamicObjectTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SortSimpleDataDescending()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "SortSourceNonGeneric"
            );
            source2Columns.InsertTestData();
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(
                Connection,
                "SortSourceNonGeneric"
            );

            //Act
            List<ExpandoObject> actual = new List<ExpandoObject>();
            CustomDestination<ExpandoObject> dest = new CustomDestination<ExpandoObject>(
                row => actual.Add(row)
            );

            int Comp(ExpandoObject x, ExpandoObject y)
            {
                dynamic xo = x;
                dynamic yo = y;
                return yo.Col1 - xo.Col1;
            }

            Sort<ExpandoObject> block = new Sort<ExpandoObject>(Comp);
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            List<int> expected = new List<int> { 3, 2, 1 };
            Assert.Equal(
                expected,
                actual
                    .Select(row =>
                    {
                        dynamic r = row;
                        return r.Col1;
                    })
                    .Cast<int>()
                    .ToList()
            );
        }
    }
}
