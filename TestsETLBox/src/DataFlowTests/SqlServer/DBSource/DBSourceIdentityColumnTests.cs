using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow")]
    public class DBSourceIdentityColumnTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public DBSourceIdentityColumnTests(DatabaseFixture dbFixture)
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

        private void DataFlowForIdentityColumn()
        {
            DBSource<MyPartialRow> source = new DBSource<MyPartialRow>(Connection, "dbo.Source4Cols");
            DBDestination<MyPartialRow> dest = new DBDestination<MyPartialRow>(Connection, "dbo.Destination4Cols");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }

        [Fact]
        private void IdentityColumnsAtTheBeginning()
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture("dbo.Source4Cols", identityColumnIndex: 0);
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("dbo.Destination4Cols", identityColumnIndex: 0);

            //Act
            DataFlowForIdentityColumn();

            //Assert
            dest4Columns.AssertTestData();
        }

        [Fact]
        private void IdentityColumnInTheMiddle()
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture("dbo.Source4Cols", identityColumnIndex: 1);
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("dbo.Destination4Cols", identityColumnIndex: 2);

            //Act
            DBSource<MyPartialRow> source = new DBSource<MyPartialRow>(Connection, "dbo.Source4Cols");
            DBDestination<MyPartialRow> dest = new DBDestination<MyPartialRow>(Connection, "dbo.Destination4Cols");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }

        [Fact]
        private void IdentityColumnAtTheEnd()
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture("dbo.Source4Cols", identityColumnIndex: 3);
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("dbo.Destination4Cols", identityColumnIndex: 3);

            //Act
            DBSource<MyPartialRow> source = new DBSource<MyPartialRow>(Connection, "dbo.Source4Cols");
            DBDestination<MyPartialRow> dest = new DBDestination<MyPartialRow>(Connection, "dbo.Destination4Cols");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
