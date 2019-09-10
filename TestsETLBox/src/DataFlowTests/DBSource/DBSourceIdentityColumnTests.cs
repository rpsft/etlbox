using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBSourceIdentityColumnTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceIdentityColumnTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MyPartialRow
        {
            public string Col2 { get; set; }
            public decimal? Col4 { get; set; }
        }

        private void DataFlowForIdentityColumn(IConnectionManager connection)
        {
            DBSource<MyPartialRow> source = new DBSource<MyPartialRow>(connection, "Source4Cols");
            DBDestination<MyPartialRow> dest = new DBDestination<MyPartialRow>(connection, "Destination4Cols");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }

        [Theory, MemberData(nameof(Connections))]
        private void IdentityColumnsAtTheBeginning(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "Source4Cols", identityColumnIndex: 0);
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "Destination4Cols", identityColumnIndex: 0);

            //Act
            DataFlowForIdentityColumn(connection);

            //Assert
            dest4Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        private void IdentityColumnInTheMiddle(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "Source4Cols", identityColumnIndex: 1);
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "Destination4Cols", identityColumnIndex: 2);

            //Act
            DataFlowForIdentityColumn(connection);

            //Assert
            dest4Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        private void IdentityColumnAtTheEnd(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "Source4Cols", identityColumnIndex: 3);
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "Destination4Cols", identityColumnIndex: 3);

            //Act
            DataFlowForIdentityColumn(connection);

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
