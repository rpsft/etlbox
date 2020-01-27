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
            public int Col1 { get; set; }
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

        public class MyExtendedRow
        {
            public string Col2 { get; set; }
            public long? Col3 { get; set; }
            public double Col4 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void DifferentColumnsInView(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture s4c = new FourColumnsTableFixture(connection, "dbsource_extended");
            s4c.InsertTestData();
            CreateViewTask.CreateOrAlter(connection, "DBSourceViewExtended", $"SELECT {s4c.QB}Col2{s4c.QE}, {s4c.QB}Col4{s4c.QE} FROM dbsource_extended");
            FourColumnsTableFixture d4c = new FourColumnsTableFixture(connection, "DBDestinationExtended", 1);

            //Act
            DBSource<MyExtendedRow> source = new DBSource<MyExtendedRow>(connection, "DBSourceViewExtended");
            DBDestination<MyExtendedRow> dest = new DBDestination<MyExtendedRow>(connection, "DBDestinationExtended");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d4c.AssertTestData();
        }
    }
}
