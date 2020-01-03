using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class LogTableTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");
        public LogTableTaskTests(LoggingDatabaseFixture dbFixture)
        {

        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateLogTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLogTableTask.Create(connection, "etlbox_testlog");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testlog");
            var td = TableDefinition.GetDefinitionFromTableName("etlbox_testlog", connection);
            Assert.True(td.Columns.Count == 10);
            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testlog");
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateLoadProcessTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLoadProcessTableTask.Create(connection, "etlbox_testloadprocess");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testloadprocess");
            var td = TableDefinition.GetDefinitionFromTableName("etlbox_testloadprocess", connection);
            Assert.True(td.Columns.Count == 11);

            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testloadprocess");
        }
    }
}
