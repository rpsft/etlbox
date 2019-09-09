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
    public class DBSourceTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("DataFlow");

        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void DBSourceAndDestinationWithTableDefinition(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "Destination");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>()
            {
                SourceTableDefinition = source2Columns.TableDefinition,
                ConnectionManager = connection
            };
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>()
            {
                DestinationTableDefinition = dest2Columns.TableDefinition,
                ConnectionManager = connection
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void SqlWithSelectStar()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(SqlConnection, "Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(SqlConnection, "Destination");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>()
            {
                Sql = $@"SELECT * FROM Source",
                ConnectionManager = SqlConnection
            };
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(SqlConnection, "Destination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        public class MyExtendedRow
        {
            [ColumnMap("Col1")]
            public int Id { get; set; }
            [ColumnMap("Col3")]
            public long? Value { get; set; }
            [ColumnMap("Col4")]
            public decimal Percentage { get; set; }
            [ColumnMap("Col2")]
            public string Text { get; set; }
        }

        [Fact]
        public void ColumnMapping()
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture("Source", identityColumnIndex: 2);
            source4Columns.InsertTestData();

            //Act
            DBSource<MyExtendedRow> source = new DBSource<MyExtendedRow>(SqlConnection, "dbo.Source");
            CustomDestination<MyExtendedRow> dest = new CustomDestination<MyExtendedRow>(
                input =>
                {
                    //Assert
                    Assert.InRange(input.Id, 1, 3);
                    Assert.StartsWith("Test", input.Text);
                    if (input.Id == 1)
                        Assert.Null(input.Value);
                    else
                        Assert.True(input.Value > 0);
                    Assert.InRange(input.Percentage, 1, 2);
                });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }


    }
}
