using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class CreateLogTablesTaskTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public CreateLogTablesTaskTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
        }

        [Fact]
        public void CreateLogTables()
        {
            //Arrange
            //Act
            CreateLogTablesTask.CreateLog(Connection);
            //Assert
            Assert.Equal(1,RowCountTask.Count(Connection, "sys.procedures",
                "type = 'P' and name = 'StartLoadProcess' and schema_id = schema_id('etl')"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.procedures",
                "type = 'P' and name = 'EndLoadProcess' and schema_id = schema_id('etl')"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.procedures",
                "type = 'P' and name = 'AbortLoadProcess' and schema_id = schema_id('etl')"));
        }
    }
}
