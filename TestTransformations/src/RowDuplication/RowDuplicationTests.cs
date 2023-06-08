using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.RowDuplication
{
    [Collection("DataFlow")]
    public class RowDuplicationTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void NoParameter()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "RowDuplicationSource"
            );
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "RowDuplicationSource"
            );
            RowDuplication<MySimpleRow> duplication = new RowDuplication<MySimpleRow>();
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo(duplication);
            duplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3")
            );
        }

        [Fact]
        public void DuplicateTwice()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "RowDuplicationSource"
            );
            source2Columns.InsertTestData();

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "RowDuplicationSource"
            );
            RowDuplication<MySimpleRow> duplication = new RowDuplication<MySimpleRow>(2);
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo(duplication);
            duplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3")
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

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "RowDuplicationSource"
            );
            RowDuplication<MySimpleRow> duplication = new RowDuplication<MySimpleRow>(
                row => row.Col1 == 1 || row.Col2 == "Test3"
            );
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo(duplication);
            duplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3")
            );
        }
    }
}
