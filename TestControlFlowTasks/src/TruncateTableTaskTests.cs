using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class TruncateTableTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> Access => Config.AccessConnection("ControlFlow");
        public TruncateTableTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))
               , MemberData(nameof(Access))]
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
