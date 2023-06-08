using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.RowDuplication
{
    [Collection("DataFlow")]
    public class RowDuplicationDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void NoParameter()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "RowDuplicationSource"
            );
            source2Columns.InsertTestData();

            DbSource source = new DbSource(SqlConnection, "RowDuplicationSource");
            ALE.ETLBox.DataFlow.RowDuplication duplication =
                new ALE.ETLBox.DataFlow.RowDuplication();
            MemoryDestination dest = new MemoryDestination();

            //Act
            source.LinkTo(duplication);
            duplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 1 && (row as dynamic).Col2 == "Test1");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 1 && (row as dynamic).Col2 == "Test1");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 2 && (row as dynamic).Col2 == "Test2");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 2 && (row as dynamic).Col2 == "Test2");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 3 && (row as dynamic).Col2 == "Test3");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 3 && (row as dynamic).Col2 == "Test3");
                }
            );
        }

        [Fact]
        public void WithPredicate()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "RowDuplicationSource"
            );
            source2Columns.InsertTestData();

            DbSource source = new DbSource(SqlConnection, "RowDuplicationSource");
            ALE.ETLBox.DataFlow.RowDuplication duplication =
                new ALE.ETLBox.DataFlow.RowDuplication(row =>
                {
                    dynamic r = row;
                    return r.Col1 == 1 || r.Col2 == "Test3";
                });
            MemoryDestination dest = new MemoryDestination();

            //Act
            source.LinkTo(duplication);
            duplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 1 && (row as dynamic).Col2 == "Test1");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 1 && (row as dynamic).Col2 == "Test1");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 2 && (row as dynamic).Col2 == "Test2");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 3 && (row as dynamic).Col2 == "Test3");
                },
                row =>
                {
                    Assert.True((row as dynamic).Col1 == 3 && (row as dynamic).Col2 == "Test3");
                }
            );
        }
    }
}
