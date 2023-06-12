using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.CrossJoinTests
{
    public class CrossJoinDynamicObjectTests : TransformationsTestBase
    {
        public CrossJoinDynamicObjectTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void DynamicObjectJoin()
        {
            //Arrange
            TwoColumnsTableFixture table1 = new TwoColumnsTableFixture(
                SqlConnection,
                "CrossJoinSource1"
            );
            table1.InsertTestData();
            TwoColumnsTableFixture table2 = new TwoColumnsTableFixture(
                SqlConnection,
                "CrossJoinSource2"
            );
            table2.InsertTestData();
            DbSource<ExpandoObject> source1 = new DbSource<ExpandoObject>(
                SqlConnection,
                "CrossJoinSource1"
            );
            DbSource<ExpandoObject> source2 = new DbSource<ExpandoObject>(
                SqlConnection,
                "CrossJoinSource2"
            );
            MemoryDestination dest = new MemoryDestination();

            CrossJoin crossJoin = new CrossJoin(
                (data1, _) =>
                {
                    dynamic d1 = data1;
                    dynamic d2 = data1;
                    dynamic res = new ExpandoObject();
                    res.Val = d1.Col1 + d2.Col2;
                    return res;
                }
            );

            //Act
            source1.LinkTo(crossJoin.InMemoryTarget);
            source2.LinkTo(crossJoin.PassingTarget);
            crossJoin.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(9, dest.Data.Count);
        }
    }
}
