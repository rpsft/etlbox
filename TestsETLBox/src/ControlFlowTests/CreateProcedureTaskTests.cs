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
        //public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("ControlFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public CreateProcedureTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void CreateProcedure(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateProcedureTask.CreateOrAlter(connection, "Proc1", "SELECT 1 AS Test");
            //Assert
            IfProcedureExistsTask.IsExisting(connection, "Proc1");
        }

        [Theory, MemberData(nameof(Connections))]
        public void AlterProcedure(IConnectionManager connection)
        {
            //Arrange
            CreateProcedureTask.CreateOrAlter(connection, "dbo.Proc2", "SELECT 1 AS Test");
            Assert.Equal(1, RowCountTask.Count(connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc2') AND create_date = modify_date"));
            //Act
            CreateProcedureTask.CreateOrAlter(connection, "dbo.Proc2", "SELECT 5 AS Test");
            //Assert
            Assert.Equal(1, RowCountTask.Count(connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc2') AND create_date <> modify_date"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateProcedureWithParameter(IConnectionManager connection)
        {
            //Arrange
            List<ProcedureParameter> pars = new List<ProcedureParameter>() {
                new ProcedureParameter("Par1", "varchar(10)"),
                new ProcedureParameter("Par2", "int", "7"),
            };
            //Act
            CreateProcedureTask.CreateOrAlter(connection, "dbo.Proc3", "SELECT 1 AS Test", pars);
            //Assert
            Assert.Equal(1, RowCountTask.Count(connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc3')"));
            Assert.Equal(2, RowCountTask.Count(connection, "sys.parameters",
                "object_id = object_id('dbo.Proc3')"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreatProcedureWithProcedureObject(IConnectionManager connection)
        {
            //Arrange
            List<ProcedureParameter> pars = new List<ProcedureParameter>() {
                new ProcedureParameter("Par1", "varchar(10)"),
                new ProcedureParameter("Par2", "int", "7"),
            };
            ProcedureDefinition procDef = new ProcedureDefinition("dbo.Proc4", "SELECT 1 AS Test", pars);
            //Act
            CreateProcedureTask.CreateOrAlter(connection, procDef);
            //Assert
            Assert.Equal(1, RowCountTask.Count(connection, "sys.objects",
                "type = 'P' AND object_id = object_id('dbo.Proc4')"));
            Assert.Equal(2, RowCountTask.Count(connection, "sys.parameters",
                "object_id = object_id('dbo.Proc4')"));
       }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => CreateDatabaseTask.Create(Config.SQLiteConnection.ConnectionManager("ControlFlow"), "Test")
                );
        }
    }
}
