using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.Logging;
using ALE.ETLBoxTests.NonParallel.src;
using ALE.ETLBoxTests.NonParallel.src.Fixtures;

namespace ALE.ETLBoxTests.NonParallel.src.Logging.ErrorTable
{
    public sealed class ErrorTableTaskTests : NonParallelTestBase, IDisposable
    {
        public ErrorTableTaskTests(LoggingDatabaseFixture fixture)
            : base(fixture) { }

        public void Dispose()
        {
            ETLBox.src.Toolbox.ControlFlow.ControlFlow.ClearSettings();
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void CreateErrorTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateErrorTableTask.Create(connection, "etlbox_error");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_error");
            var td = TableDefinition.GetDefinitionFromTableName(connection, "etlbox_error");
            Assert.True(td.Columns.Count == 3);
            //Cleanup
            DropTableTask.Drop(connection, "etlbox_error");
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void ReCreateErrorTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateTableTask.Create(
                connection,
                "etlbox_error",
                new List<TableColumn> { new("Col1", "INT") }
            );
            CreateErrorTableTask.DropAndCreate(connection, "etlbox_error");
            //Assert
            var td = TableDefinition.GetDefinitionFromTableName(connection, "etlbox_error");
            Assert.True(td.Columns.Count == 3);
            //Cleanup
            DropTableTask.Drop(connection, "etlbox_error");
        }
    }
}
