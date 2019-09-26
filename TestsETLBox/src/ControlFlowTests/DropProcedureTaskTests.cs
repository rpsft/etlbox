using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class DropProcedureTaskTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("ControlFlow");

        public DropProcedureTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void DropProcedure()
        {
            //Arrange
            CreateProcedureTask.CreateOrAlter(SqlConnection, "DropProc1", "SELECT 1");
            Assert.True(IfTableOrViewExistsTask.IsExisting(SqlConnection, "DropProc1"));

            //Act
            DropProcedureTask.Drop(SqlConnection, "DropProc1");

            //Assert
            Assert.False(IfTableOrViewExistsTask.IsExisting(SqlConnection, "DropProc1"));
        }

        public SQLiteConnectionManager SQLiteConnection => Config.SQLiteConnection.ConnectionManager("ControlFlow");

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropProcedureTask.Drop(SQLiteConnection, "Test")
                );
        }
    }
}
