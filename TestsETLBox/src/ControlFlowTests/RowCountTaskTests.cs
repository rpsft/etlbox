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
    public class RowCountTaskTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("ControlFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public RowCountTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void NormalCount(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture tableDef = new TwoColumnsTableFixture(connection, "RowCountTest");
            tableDef.InsertTestData();
            //Act
            int? actual = RowCountTask.Count(connection, "RowCountTest");
            //Assert
            Assert.Equal(3, actual);
        }

        [Theory, MemberData(nameof(Connections))]
        public void CountWithCondition(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture tableDef = new TwoColumnsTableFixture(connection, "RowCountTest");
            tableDef.InsertTestData();
            //Act
            int? actual = RowCountTask.Count(connection, "RowCountTest", "Col1 = 2");
            //Assert
            Assert.Equal(1, actual );
        }

        [Fact]
        public void SqlServerQuickQueryMode()
        {
            //Arrange
            TwoColumnsTableFixture tableDef = new TwoColumnsTableFixture(SqlConnection, "RowCountTest");
            tableDef.InsertTestData();
            //Act
            int? actual = RowCountTask.Count(SqlConnection, "RowCountTest", RowCountOptions.QuickQueryMode);
            //Assert
            Assert.Equal(3, actual);
        }


    }
}
