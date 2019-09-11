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
    public class CreateProcedureTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("ControlFlow");

        public CreateProcedureTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void CreateProcedure()
        {
            //Arrange
            //Act
            CreateProcedureTask.CreateOrAlter(Connection, "dbo.Proc1", "SELECT 1 AS Test");
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc1')"));
        }

        [Fact]
        public void AlterProcedure()
        {
            //Arrange
            CreateProcedureTask.CreateOrAlter(Connection, "dbo.Proc2", "SELECT 1 AS Test");
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc2') AND create_date = modify_date"));
            //Act
            CreateProcedureTask.CreateOrAlter(Connection, "dbo.Proc2", "SELECT 5 AS Test");
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc2') AND create_date <> modify_date"));
        }

        [Fact]
        public void CreatProcedureWithParameter()
        {
            //Arrange
            List<ProcedureParameter> pars = new List<ProcedureParameter>() {
                new ProcedureParameter("Par1", "varchar(10)"),
                new ProcedureParameter("Par2", "int", "7"),
            };
            //Act
            CreateProcedureTask.CreateOrAlter(Connection, "dbo.Proc3", "SELECT 1 AS Test", pars);
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc3')"));
            Assert.Equal(2, RowCountTask.Count(Connection, "sys.parameters",
                "object_id = object_id('dbo.Proc3')"));
        }

        [Fact]
        public void CreatProcedureWithProcedureObject()
        {
            //Arrange
            List<ProcedureParameter> pars = new List<ProcedureParameter>() {
                new ProcedureParameter("Par1", "varchar(10)"),
                new ProcedureParameter("Par2", "int", "7"),
            };
            ProcedureDefinition procDef = new ProcedureDefinition("dbo.Proc4", "SELECT 1 AS Test", pars);
            //Act
            CreateProcedureTask.CreateOrAlter(Connection, procDef);
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc4')"));
            Assert.Equal(2, RowCountTask.Count(Connection, "sys.parameters",
                "object_id = object_id('dbo.Proc4')"));
       }

        public SQLiteConnectionManager SQLiteConnection => Config.SQLiteConnection.ConnectionManager("ControlFlow");

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => CreateDatabaseTask.Create(SQLiteConnection, "Test")
                );
        }
    }
}
