using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database.SqlServer;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src.SqlServer
{
    public class CalculateDatabaseHashTaskTests : ControlFlowTestBase
    {
        public CalculateDatabaseHashTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void HashCalculationForOneTable()
        {
            //Arrange
            CreateSchemaTask.Create(SqlConnection, "hash");
            var columns = new List<TableColumn> { new("value", "int") };
            CreateTableTask.Create(SqlConnection, "DatabaseHash", columns);

            //Act
            var hash = CalculateDatabaseHashTask.Calculate(
                SqlConnection,
                new List<string> { "hash" }
            );
            var hashAgain = CalculateDatabaseHashTask.Calculate(
                SqlConnection,
                new List<string> { "hash" }
            );

            //Assert
            Assert.Equal(hash, hashAgain);
            Assert.Equal("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", hash);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () =>
                    CalculateDatabaseHashTask.Calculate(
                        SqliteConnection,
                        new List<string> { "hash" }
                    )
            );
        }
    }
}
