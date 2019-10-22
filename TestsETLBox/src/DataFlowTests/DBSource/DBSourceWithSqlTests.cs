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

            //Assert
            d2c.AssertTestData();
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
                Sql = $@"SELECT CASE WHEN ISNULL(Col1,'') IS NOT NULL THEN Col1 ELSE Col1 END AS Col1, 
Col2 
FROM SourceSql",
                ConnectionManager = connection
            };
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DestinationSql");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

        [Fact]
        public void SqlWithSchemaName()
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "dbo.SourceSqlSchemaName");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "dbo.DestinationSqlSchemaName");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>()
            {
                Sql = $@"SELECT sou.Col1, sou.Col2 FROM dbo.SourceSqlSchemaName AS sou",
                ConnectionManager = SqlConnection
            };
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(SqlConnection, "dbo.DestinationSqlSchemaName");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

    }
}
