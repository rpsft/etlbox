using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks.SqlServer
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
            List<TableColumn> columns = new List<TableColumn> { new("value", "int") };
            CreateTableTask.Create(SqlConnection, "DatabaseHash", columns);

            //Act
            string hash = CalculateDatabaseHashTask.Calculate(
                SqlConnection,
                new List<string> { "hash" }
            );
            string hashAgain = CalculateDatabaseHashTask.Calculate(
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
