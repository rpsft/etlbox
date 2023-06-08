using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.MergeJoin
{
    [Collection("DataFlow")]
    public class MergeJoinStringArrayTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void MergeJoinUsingOneObject()
        {
            //Arrange
            TwoColumnsTableFixture source1Table = new TwoColumnsTableFixture(
                "MergeJoinNonGenericSource1"
            );
            source1Table.InsertTestData();
            TwoColumnsTableFixture source2Table = new TwoColumnsTableFixture(
                "MergeJoinNonGenericSource2"
            );
            source2Table.InsertTestDataSet2();
            var _ = new TwoColumnsTableFixture("MergeJoinNonGenericDestination");

            DbSource<string[]> source1 = new DbSource<string[]>(
                Connection,
                "MergeJoinNonGenericSource1"
            );
            DbSource<string[]> source2 = new DbSource<string[]>(
                Connection,
                "MergeJoinNonGenericSource2"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                Connection,
                "MergeJoinNonGenericDestination"
            );

            //Act
            MergeJoin<string[]> join = new MergeJoin<string[]>(
                (inputRow1, inputRow2) =>
                {
                    inputRow1[0] = (int.Parse(inputRow1[0]) + int.Parse(inputRow2[0])).ToString();
                    inputRow1[1] += inputRow2[1];
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
            Assert.Equal(3, RowCountTask.Count(Connection, "MergeJoinNonGenericDestination"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    "MergeJoinNonGenericDestination",
                    "Col1 = 5 AND Col2='Test1Test4'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    "MergeJoinNonGenericDestination",
                    "Col1 = 7 AND Col2='Test2Test5'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    Connection,
                    "MergeJoinNonGenericDestination",
                    "Col1 = 9 AND Col2='Test3Test6'"
                )
            );
        }
    }
}
