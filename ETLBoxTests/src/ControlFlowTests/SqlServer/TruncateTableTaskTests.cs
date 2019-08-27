using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    public class TruncateTableTaskFixture
    {
        public TruncateTableTaskFixture()
        {
            SqlTask.ExecuteNonQuery(Config.SqlConnectionManager("ControlFlow")
                , "Create test data table"
                , $@"
CREATE TABLE TruncateTableTest
(
    Col1 INT NULL
)
INSERT INTO TruncateTableTest
SELECT * FROM
(VALUES (1), (2), (3)) AS MyTable(v)");
        }
    }

    [Collection("Sql Server ControlFlow")]
    public class TruncateTableTaskTests : IClassFixture<TruncateTableTaskFixture>
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public TruncateTableTaskTests(DatabaseFixture dbFixture, TruncateTableTaskFixture rcfixture)
        { }


        [Fact]
        public void Truncate()
        {
            //Arrange
            Assert.Equal(3, RowCountTask.Count(Connection, "TruncateTableTest"));
            //Act
            TruncateTableTask.Truncate(Connection, "TruncateTableTest");
            //Assert
            Assert.Equal(0, RowCountTask.Count(Connection, "TruncateTableTest"));
        }

    }
}
