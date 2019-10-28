using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBSourceWithSqlTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DBSourceWithSqlTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
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
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>()
            {
                Sql = $@"SELECT * FROM {s2c.QB}SourceSelectStar{s2c.QE}",
                ConnectionManager = connection
            };
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DestinationSelectStar");
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
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>()
            {
                Sql = $@"SELECT CASE WHEN {s2c.QB}Col1{s2c.QE} IS NOT NULL THEN {s2c.QB}Col1{s2c.QE} ELSE {s2c.QB}Col1{s2c.QE} END AS {s2c.QB}Col1{s2c.QE}, 
{s2c.QB}Col2{s2c.QE} 
FROM {s2c.QB}SourceSql{s2c.QE}",
                ConnectionManager = connection
            };
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DestinationSql");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }
    }
}
