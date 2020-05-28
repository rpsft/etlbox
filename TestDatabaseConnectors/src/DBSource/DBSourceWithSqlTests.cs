using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbSourceWithSqlTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DbSourceWithSqlTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public long Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void SqlWithSelectStar(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceSelectStar");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DestinationSelectStar");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>()
            {
                Sql = $@"SELECT * FROM {s2c.QB}SourceSelectStar{s2c.QE}",
                ConnectionManager = connection
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "DestinationSelectStar");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            d2c.AssertTestData();
            //Assert

        }

        [Theory, MemberData(nameof(Connections))]
        public void SqlWithNamedColumns(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceSql");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DestinationSql");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>()
            {
                Sql = $@"SELECT CASE WHEN {s2c.QB}Col1{s2c.QE} IS NOT NULL THEN {s2c.QB}Col1{s2c.QE} ELSE {s2c.QB}Col1{s2c.QE} END AS {s2c.QB}Col1{s2c.QE}, 
{s2c.QB}Col2{s2c.QE} 
FROM {s2c.QB}SourceSql{s2c.QE}",
                ConnectionManager = connection
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "DestinationSql");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }
    }
}
