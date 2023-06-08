using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.MergeJoin
{
    [Collection("DataFlow")]
    public class MergeJoinDynamicObjectTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

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
                Connection,
                "MergeJoinDynamicSource1"
            );
            DbSource<ExpandoObject> source2 = new DbSource<ExpandoObject>(
                Connection,
                "MergeJoinDynamicSource2"
            );
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                Connection,
                "MergeJoinDynamicDestination"
            );

            //Act
            MergeJoin<ExpandoObject> join = new MergeJoin<ExpandoObject>(
                (inputRow1, inputRow2) =>
                {
                    dynamic ir1 = inputRow1;
                    dynamic ir2 = inputRow2;
                    ir1.Col1 = ir1.Col1 + ir2.Col1;
                    ir1.Col2 = ir1.Col2 + ir2.Col2;
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
            Assert.Equal(3, RowCountTask.Count(Connection, "MergeJoinDynamicDestination"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    "MergeJoinDynamicDestination",
                    "Col1 = 5 AND Col2='Test1Test4'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    "MergeJoinDynamicDestination",
                    "Col1 = 7 AND Col2='Test2Test5'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    "MergeJoinDynamicDestination",
                    "Col1 = 9 AND Col2='Test3Test6'"
                )
            );
        }
    }
}
