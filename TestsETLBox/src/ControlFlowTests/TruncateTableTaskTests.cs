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
    public class TruncateTableTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public TruncateTableTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void Truncate(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture tableDef = new TwoColumnsTableFixture(connection, "TruncateTableTest");
            tableDef.InsertTestData();
            Assert.Equal(3, RowCountTask.Count(connection, "TruncateTableTest"));
            //Act
            TruncateTableTask.Truncate(connection, "TruncateTableTest");
            //Assert
            Assert.Equal(0, RowCountTask.Count(connection, "TruncateTableTest"));
        }

    }
}
