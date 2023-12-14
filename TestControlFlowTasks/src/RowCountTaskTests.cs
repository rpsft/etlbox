using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestControlFlowTasks.src
{
    public class RowCountTaskTests : ControlFlowTestBase
    {
        public RowCountTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(AllSqlConnections)), MemberData(nameof(AccessConnection))]
        public void NormalCount(IConnectionManager connection)
        {
            //Arrange
            var tableDef = new TwoColumnsTableFixture(
                connection,
                "RowCountTest"
            );
            tableDef.InsertTestData();
            //Act
            var actual = RowCountTask.Count(connection, "RowCountTest");
            //Assert
            Assert.Equal(3, actual);
        }

        [Theory, MemberData(nameof(AllSqlConnections)), MemberData(nameof(AccessConnection))] //If access fails with "Internal OLE Automation error", download and install: https://www.microsoft.com/en-us/download/confirmation.aspx?id=50040
        //see also: https://stackoverflow.com/questions/54632928/internal-ole-automation-error-in-ms-access-using-oledb
        public void CountWithCondition(IConnectionManager connection)
        {
            //Arrange
            var tc = new TwoColumnsTableFixture(connection, "RowCountTest");
            tc.InsertTestData();
            //Act
            var actual = RowCountTask.Count(connection, "RowCountTest", $"{tc.QB}Col1{tc.QE} = 2");
            //Assert
            Assert.Equal(1, actual);
        }

        [Fact]
        public void SqlServerQuickQueryMode()
        {
            //Arrange
            var tableDef = new TwoColumnsTableFixture(
                SqlConnection,
                "RowCountTest"
            );
            tableDef.InsertTestData();
            //Act
            var actual = RowCountTask.Count(
                SqlConnection,
                "RowCountTest",
                RowCountOptions.QuickQueryMode
            );
            //Assert
            Assert.Equal(3, actual);
        }
    }
}
