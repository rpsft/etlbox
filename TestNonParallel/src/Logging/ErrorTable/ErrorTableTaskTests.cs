using System.Collections.Generic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using TestShared.Helper;

namespace ALE.ETLBoxTests.NonParallel.Logging.ErrorTable
{
    [Collection("Logging")]
    public class ErrorTableTaskTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("Logging");

        public void Dispose()
        {
            ETLBox.ControlFlow.ControlFlow.ClearSettings();
        }

        [Theory, MemberData(nameof(Connections))]
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

        [Theory, MemberData(nameof(Connections))]
        public void ReCreateErrorTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateTableTask.Create(
                connection,
                "etlbox_error",
                new List<TableColumn> { new TableColumn("Col1", "INT") }
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
