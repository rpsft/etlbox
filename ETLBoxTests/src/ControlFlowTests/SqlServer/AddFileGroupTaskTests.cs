using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.SqlServer
{
    [Collection("Sql Server ControlFlow")]
    public class AddFileGroupTaskTests
    {
        public AddFileGroupTaskTests(DatabaseFixture dbFixture)
        { }

        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public string DBName => Config.SqlConnectionString("ControlFlow").DBName;

        [Fact]
        public void AddFileGroup()
        {
            //Arrange
            string fgName = HashHelper.RandomString(10) + "_FG";
            Assert.Equal(0, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}'"));

            //Act
            AddFileGroupTask.AddFileGroup(Connection, fgName, DBName, "2048KB", "2048KB", false);

            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.sysfiles", $"name = '{fgName}'"));
        }

        [Fact]
        public void AddDefaultFileGroup()
        {
            //Arrange
            string fgName = HashHelper.RandomString(10) + "_FG";
            Assert.Equal(0, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}' AND is_default = 1"));

            //Act
            AddFileGroupTask.AddFileGroup(Connection, fgName, DBName, "5MB", "5MB", true);

            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.filegroups", $"name = '{fgName}' AND is_default = 1"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.sysfiles", $"name = '{fgName}'"));
        }
    }
}
