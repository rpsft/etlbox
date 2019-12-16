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

namespace ALE.ETLBoxTests.ControlFlowTests.Postgres
{
    [Collection("ControlFlow")]
    public class TableDefinitionTests
    {
        public PostgresConnectionManager PostgresConnection => Config.PostgresConnection.ConnectionManager("ControlFlow");

        public TableDefinitionTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void Identity()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(PostgresConnection, "Create table", @"
CREATE TABLE identity (
    document_id serial PRIMARY KEY,
     header_text VARCHAR (255) NOT NULL
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName("identity", PostgresConnection);

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "integer" && tc.NETDataType == typeof(Int32)
                                    && tc.IsIdentity && tc.IsPrimaryKey),
                tc => Assert.True(tc.DataType == "varchar(255)" && tc.NETDataType == typeof(String))
            );
        }

        [Fact]
        public void DateTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(PostgresConnection, "Create table", @"
CREATE TABLE datetimetypes (
    datetype DATE,
    timetype TIME,
    timetypewithtimezone TIMETZ,
    intervaltype INTERVAL,
    tswithouttimezone TIMESTAMP,
    tswithtimezone TIMESTAMPTZ
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName("datetimetypes", PostgresConnection);

            //Assert
            Assert.Collection(result.Columns,
                tc => Assert.True(tc.DataType == "date" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "time" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "timetz" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "interval" && tc.NETDataType == typeof(String)),
                tc => Assert.True(tc.DataType == "timestamp" && tc.NETDataType == typeof(DateTime)),
                tc => Assert.True(tc.DataType == "timestamptz" && tc.NETDataType == typeof(DateTime))
            );
        }

    }
}
