using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Linq;
using Xunit;

namespace ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("ControlFlow")]
    public class TableDefinitionTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("ControlFlow");

        public TableDefinitionTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void BigIntIdentity()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE BigIntIdentity (
    Id BIGINT NOT NULL PRIMARY KEY IDENTITY(100,10)
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(SqlConnection, "BigIntIdentity");

            //Assert
            Assert.Collection(result.Columns,
                           tc => Assert.True(tc.DataType == "BIGINT"
                           && tc.NETDataType == typeof(Int64)
                           && tc.IsIdentity == true
                           && tc.IdentityIncrement == 10
                           && tc.IdentitySeed == 100
                           )
                       );
        }

        [Fact]
        public void NumbericDataTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE NumericDataTypes (
    Id BIGINT NOT NULL,
    Col1 NUMERIC,
    Col2 BIT,
    Col3 SMALLINT,
    Col4 DECIMAL,
    Col5 SMALLMONEY,
    Col6 INT,
    Col7 TINYINT,
    Col8 MONEY,
    Col9 FLOAT,
    Col10 REAL
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(SqlConnection, "NumericDataTypes");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "BIGINT" && tc.NETDataType == typeof(Int64)),
                tc => Assert.True(tc.DataType == "NUMERIC(18,0)" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "BIT" && tc.NETDataType == typeof(Boolean)),
                tc => Assert.True(tc.DataType == "SMALLINT" && tc.NETDataType == typeof(Int16)),
                tc => Assert.True(tc.DataType == "DECIMAL(18,0)" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "SMALLMONEY" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "INT" && tc.NETDataType == typeof(Int32)),
                tc => Assert.True(tc.DataType == "TINYINT" && tc.NETDataType == typeof(UInt16)),
                tc => Assert.True(tc.DataType == "MONEY" && tc.NETDataType == typeof(Decimal)),
                tc => Assert.True(tc.DataType == "FLOAT" && tc.NETDataType == typeof(Double)),
                tc => Assert.True(tc.DataType == "REAL" && tc.NETDataType == typeof(Double))
            );
        }

        [Fact]
        public void DateTimeTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE ReadTableDefinition (
    Col11 DATE,
    --Col12 DATETIMEOFFSET,
    Col13 DATETIME2,
    Col14 SMALLDATETIME,
    Col15 DATETIME,
    Col16 TIME

)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(SqlConnection, "ReadTableDefinition");

            //Assert
            Assert.True(result.Columns.All(tc => tc.NETDataType == typeof(DateTime)));
            Assert.True(result.Columns.Count == 5);
        }


        [Fact]
        public void TypesWithLengthOrPrecision()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE LengthOrPrecisionTypes (    
    Col1 DECIMAL (12,3),
    Col2 NVARCHAR(100),
    Col3 VARCHAR(10),
    Col4 CHAR(4),
    Col5 NCHAR(4),
    Col6 NUMERIC(3,2),    
    Col9 BINARY(10),
    Col10 VARBINARY(20)
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(SqlConnection, "LengthOrPrecisionTypes");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "DECIMAL(12,3)" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "NVARCHAR(100)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "VARCHAR(10)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "CHAR(4)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "NCHAR(4)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "NUMERIC(3,2)" && tc.NETDataType == typeof(decimal)),
                tc => Assert.True(tc.DataType == "BINARY(10)" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "VARBINARY(20)" && tc.NETDataType == typeof(string))
            );
        }

        [Fact]
        public void TextTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(SqlConnection, "Create table", @"
CREATE TABLE TextTypes (    
    Col1 TEXT,
    Col2 NTEXT,
    Col3 IMAGE    
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(SqlConnection, "TextTypes");

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "TEXT" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "NTEXT" && tc.NETDataType == typeof(string)),
                tc => Assert.True(tc.DataType == "IMAGE" && tc.NETDataType == typeof(string))
            );
        }
    }
}
