using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbSourceViewsTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbSourceViewsTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleFlow(IConnectionManager conn)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(conn, "dbsource_simple");
            source2Columns.InsertTestData();
            CreateViewTask.CreateOrAlter(conn, "DbSourceView", $"SELECT * FROM {conn.QB}dbsource_simple{conn.QE}");
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(conn, "DbDestinationSimple");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(conn, "DbSourceView");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(conn, "DbDestinationSimple");

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
        public void DifferentColumnsInView(IConnectionManager conn)
        {
            //Arrange
            FourColumnsTableFixture s4c = new FourColumnsTableFixture(conn, "dbsource_extended");
            s4c.InsertTestData();
            CreateViewTask.CreateOrAlter(conn, "DbSourceViewExtended", $"SELECT {conn.QB}Col2{conn.QE}, {conn.QB}Col4{conn.QE} FROM {conn.QB}dbsource_extended{conn.QE}");
            FourColumnsTableFixture d4c = new FourColumnsTableFixture(conn, "DbDestinationExtended", 1);

            //Act
            DbSource<MyExtendedRow> source = new DbSource<MyExtendedRow>(conn, "DbSourceViewExtended");
            DbDestination<MyExtendedRow> dest = new DbDestination<MyExtendedRow>(conn, "DbDestinationExtended");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d4c.AssertTestData();
        }
    }
}
