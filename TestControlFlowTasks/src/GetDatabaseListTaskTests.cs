using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class GetDatabaseListTaskTests : ControlFlowTestBase
    {
        public GetDatabaseListTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        private static string DBName => SqlConnection.ConnectionString.DbName;

        [Fact]
        public void GetDatabaseList()
        {
            //Arrange

            //Act
            List<string> allDatabases = GetDatabaseListTask.List(SqlConnection);

            //Assert
            Assert.True(allDatabases.Count >= 1);
            Assert.Contains(allDatabases, name => name == DBName);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => GetDatabaseListTask.List(SqliteConnection)
            );
        }
    }
}
