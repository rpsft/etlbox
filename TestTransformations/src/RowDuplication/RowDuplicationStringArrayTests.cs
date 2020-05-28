using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowDuplicationStringArrayTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowDuplicationStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void DataIsInList()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("RowDuplicationStringArraySource");
            source2Columns.InsertTestData();

            DbSource<string[]> source = new DbSource<string[]>(SqlConnection, "RowDuplicationStringArraySource");
            RowDuplication<string[]> duplication = new RowDuplication<string[]>();
            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();

            //Act
            source.LinkTo(duplication);
            duplication.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data,
                d => Assert.True(d[0] == "1" && d[1] == "Test1"),
                d => Assert.True(d[0] == "1" && d[1] == "Test1"),
                d => Assert.True(d[0] == "2" && d[1] == "Test2"),
                d => Assert.True(d[0] == "2" && d[1] == "Test2"),
                d => Assert.True(d[0] == "3" && d[1] == "Test3"),
                d => Assert.True(d[0] == "3" && d[1] == "Test3")
            );
        }


    }
}
