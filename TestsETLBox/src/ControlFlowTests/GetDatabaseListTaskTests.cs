using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class GetDatabaseListTaskTests
    {
        public GetDatabaseListTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public string DBName => Config.SqlConnectionString("ControlFlow").DBName;

        [Fact]
        public void GetDatabaseList()
        {
            //Arrange

            //Act
            List<string> allDatabases = GetDatabaseListTask.List(Connection);

            //Assert
            Assert.True(allDatabases.Count >= 1);
            Assert.Contains(allDatabases, name => name == DBName);

        }
    }
}
