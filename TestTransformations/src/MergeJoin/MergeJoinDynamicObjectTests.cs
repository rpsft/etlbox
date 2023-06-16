using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.SharedFixtures;
using TestTransformations.Fixtures;

namespace TestTransformations.MergeJoin
{
    public class MergeJoinDynamicObjectTests : TransformationsTestBase
    {
        public MergeJoinDynamicObjectTests(TransformationsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void MergeJoinUsingOneObject()
        {
            //Arrange
            TwoColumnsTableFixture source1Table = new TwoColumnsTableFixture(
                "MergeJoinDynamicSource1"
            );
            source1Table.InsertTestData();
            TwoColumnsTableFixture source2Table = new TwoColumnsTableFixture(
                "MergeJoinDynamicSource2"
            );
            source2Table.InsertTestDataSet2();
            var _ = new TwoColumnsTableFixture("MergeJoinDynamicDestination");

            DbSource<ExpandoObject> source1 = new DbSource<ExpandoObject>(
                SqlConnection,
                "MergeJoinDynamicSource1"
            );
            DbSource<ExpandoObject> source2 = new DbSource<ExpandoObject>(
                SqlConnection,
                "MergeJoinDynamicSource2"
            );
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "MergeJoinDynamicDestination"
            );

            //Act
            MergeJoin<ExpandoObject> join = new MergeJoin<ExpandoObject>(
                (inputRow1, inputRow2) =>
                {
                    dynamic ir1 = inputRow1;
                    dynamic ir2 = inputRow2;
                    ir1.Col1 += ir2.Col1;
                    ir1.Col2 += ir2.Col2;
                    return inputRow1;
                }
            );
            source1.LinkTo(join.Target1);
            source2.LinkTo(join.Target2);
            join.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "MergeJoinDynamicDestination"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "MergeJoinDynamicDestination",
                    "Col1 = 5 AND Col2='Test1Test4'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "MergeJoinDynamicDestination",
                    "Col1 = 7 AND Col2='Test2Test5'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "MergeJoinDynamicDestination",
                    "Col1 = 9 AND Col2='Test3Test6'"
                )
            );
        }
    }
}
