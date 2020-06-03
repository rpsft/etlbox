using ETLBox.ConnectionManager;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceIdentityColumTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvSourceIdentityColumTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void IdentityAtPosition1()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 0);
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }

        [Fact]
        public void IdentityInTheMiddle()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 2);
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }


        [Fact]
        public void IdentityAtTheEnd()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 3);
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
