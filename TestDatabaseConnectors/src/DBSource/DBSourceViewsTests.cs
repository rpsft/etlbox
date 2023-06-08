using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBSource
{
    [Collection("DataFlow")]
    public class DbSourceViewsTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleFlow(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                connection,
                "dbsource_simple"
            );
            source2Columns.InsertTestData();
            CreateViewTask.CreateOrAlter(
                connection,
                "DbSourceView",
                "SELECT * FROM dbsource_simple"
            );
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                connection,
                "DbDestinationSimple"
            );

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "DbSourceView");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                connection,
                "DbDestinationSimple"
            );

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
            FourColumnsTableFixture s4c = new FourColumnsTableFixture(
                connection,
                "dbsource_extended"
            );
            s4c.InsertTestData();
            CreateViewTask.CreateOrAlter(
                connection,
                "DbSourceViewExtended",
                $"SELECT {s4c.QB}Col2{s4c.QE}, {s4c.QB}Col4{s4c.QE} FROM dbsource_extended"
            );
            FourColumnsTableFixture d4c = new FourColumnsTableFixture(
                connection,
                "DbDestinationExtended",
                1
            );

            //Act
            DbSource<MyExtendedRow> source = new DbSource<MyExtendedRow>(
                connection,
                "DbSourceViewExtended"
            );
            DbDestination<MyExtendedRow> dest = new DbDestination<MyExtendedRow>(
                connection,
                "DbDestinationExtended"
            );

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d4c.AssertTestData();
        }
    }
}
