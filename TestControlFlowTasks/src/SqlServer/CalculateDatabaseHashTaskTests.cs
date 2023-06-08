using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;

namespace TestControlFlowTasks.SqlServer
{
    [Collection("ControlFlow")]
    public class CalculateDatabaseHashTaskTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("ControlFlow");

        [Fact]
        public void HashCalculationForOneTable()
        {
            //Arrange
            CreateSchemaTask.Create(Connection, "hash");
            List<TableColumn> columns = new List<TableColumn> { new TableColumn("value", "int") };
            CreateTableTask.Create(Connection, "DatabaseHash", columns);

            //Act
            string hash = CalculateDatabaseHashTask.Calculate(
                Connection,
                new List<string> { "hash" }
            );
            string hashAgain = CalculateDatabaseHashTask.Calculate(
                Connection,
                new List<string> { "hash" }
            );

            //Assert
            Assert.Equal(hash, hashAgain);
            Assert.Equal("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", hash);
        }

        public SQLiteConnectionManager SQLiteConnection =>
            Config.SQLiteConnection.ConnectionManager("ControlFlow");

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () =>
                    CalculateDatabaseHashTask.Calculate(
                        SQLiteConnection,
                        new List<string> { "hash" }
                    )
            );
        }
    }
}
