using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class IfExistsTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public IfExistsTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]
        public void IfTableExists()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(Config.SqlConnectionManager("ControlFlow")
               , "Create test data table"
               , $@"CREATE TABLE ExistTableTest ( Col1 INT NULL )");

            //Act
            var exists = IfExistsTask.IsExisting(Connection, "ExistTableTest");

            //Assert
            Assert.True(exists);
        }
    }
}
