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
    public class DBSourceViewsTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceViewsTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public long Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleFlow(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "dbsource_simple");
            source2Columns.InsertTestData();
            CreateViewTask.CreateOrAlter(connection, "DBSourceView", "SELECT * FROM dbsource_simple");
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBDestinationSimple");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBSourceView");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DBDestinationSimple");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void DifferentColumnsInView(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "dbsource_extended");
            source4Columns.InsertTestData();
            CreateViewTask.CreateOrAlter(connection, "DBSourceViewExtended", "SELECT Col2, Col3 FROM dbsource_simple");
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "DBDestinationExtended");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBSourceViewExtended");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DBDestinationExtended");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
