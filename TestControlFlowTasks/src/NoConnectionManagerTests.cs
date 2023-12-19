using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;

namespace TestControlFlowTasks
{
    public class NoConnectionManagerTests
    {
        [Fact]
        public void CheckSqlTask()
        {
            //Arrange
            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                SqlTask.ExecuteNonQuery("test", "SELECT 1");
            });
        }

        [Fact]
        public void CheckRowCountTask()
        {
            //Arrange
            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                RowCountTask.Count("test");
            });
        }

        [Fact]
        public void CheckCreateTableTask()
        {
            //Arrange
            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                CreateTableTask.Create("test", new List<TableColumn>());
            });
        }

        [Fact]
        public void CheckIfExistsDatabaseTask()
        {
            //Arrange
            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                IfDatabaseExistsTask.IsExisting("test");
            });
        }

        [Fact]
        public void CheckCreateSchemaTask()
        {
            //Arrange
            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                CreateSchemaTask.Create("test");
            });
        }

        [Fact]
        public void CheckDropSchemaTask()
        {
            //Arrange
            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                DropSchemaTask.DropIfExists("test");
            });
        }
    }
}
