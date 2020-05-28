using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Helper;
using ETLBox.MySql;
using ETLBoxTests.Fixtures;
using System;
using Xunit;

namespace ETLBoxTests.ControlFlowTests.MySql
{
    [Collection("ControlFlow")]
    public class TableDefinitionTests
    {
        public MySqlConnectionManager MySqlConnection => Config.MySqlConnection.ConnectionManager("ControlFlow");

        public TableDefinitionTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void AutoIncrement()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(MySqlConnection, "Create table", @"
CREATE TABLE identity (
      `Id` INT AUTO_INCREMENT NOT NULL PRIMARY KEY
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(MySqlConnection, "identity");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "int" && tc.NETDataType == typeof(Int32)
                                    && tc.IsIdentity && tc.IsPrimaryKey)
            );
        }

        [Fact]
        public void TypesWithLengthAndPrecision()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(MySqlConnection, "Create table", @"
CREATE TABLE length_and_precision (
      `Value1` VARCHAR (255) NULL,
      `Value2` NVARCHAR (200) NULL,
      `Value3` CHAR (10) NULL,
      `Value4` NCHAR (20) NULL,
      `Value5` DECIMAL (12,3) NULL,
      `Value6` NUMERIC (3,2) NULL
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(MySqlConnection, "length_and_precision");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "varchar(255)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "varchar(200)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "char(10)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "char(20)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "decimal(12,3)" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "decimal(3,2)" && tc.NETDataType == typeof(decimal))
            );
        }

        [Fact]
        public void TestComment()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(MySqlConnection, "Create table", @"
CREATE TABLE testcomment (
      `comment` INT NULL COMMENT 'test'
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(MySqlConnection, "testcomment");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "int" && tc.Comment == "test")
            );
        }
    }
}
