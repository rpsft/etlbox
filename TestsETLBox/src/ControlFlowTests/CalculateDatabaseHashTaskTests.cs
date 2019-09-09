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
    public class CalculateDatabaseHashTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public CalculateDatabaseHashTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]
        public void HashCalculationForOneTable()
        {
            //Arrange
            CreateSchemaTask.Create(Connection, "hash");
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create(Connection, "DatabaseHash", columns);

            //Act
            string hash = CalculateDatabaseHashTask.Calculate(Connection, new List<string>() { "hash" });
            string hashAgain = CalculateDatabaseHashTask.Calculate(Connection, new List<string>() { "hash" });

            //Assert
            Assert.Equal(hash, hashAgain);
            Assert.Equal("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709", hash);

        }
    }
}
