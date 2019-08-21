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
    public class RowCountTaskFixture
    {
        public RowCountTaskFixture()
        {
            SqlTask.ExecuteNonQuery(Config.SqlConnectionManager("ControlFlow")
                , "Create test data table"
                , $@"
CREATE TABLE RowCountTest
(

    Col1 INT NULL
)
INSERT INTO RowCountTest
SELECT * FROM
(VALUES (1), (2), (3)) AS MyTable(v)");
        }
    }

    [Collection("Sql Server ControlFlow")]
    public class RowCountTaskTests : IClassFixture<RowCountTaskFixture>
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public RowCountTaskTests(DatabaseFixture dbFixture, RowCountTaskFixture rcfixture)
        { }

        [Fact]
        public void NormalCount()
        {
            //Arrange
            //Act
            int? actual = RowCountTask.Count(Connection, "RowCountTest");
            //Assert
            Assert.Equal(3, actual);
        }

        [Fact]
        public void CountWithCondition()
        {
            //Arrange
            //Act
            int? actual = RowCountTask.Count(Connection,"RowCountTest", "Col1 = 2");
            //Assert
            Assert.Equal(1, actual );
        }

        [Fact]
        public void CountWithQuickQueryMode()
        {
            //Arrange
            //Act
            int? actual = RowCountTask.Count(Connection, "RowCountTest", RowCountOptions.QuickQueryMode);
            //Assert
            Assert.Equal(3, actual);
        }


    }
}
