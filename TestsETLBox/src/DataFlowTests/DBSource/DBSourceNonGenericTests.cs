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
    public class DBSourceNonGenericTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceNonGenericTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void UsingTableDefinitions(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceTableDef");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DestinationTableDef");

            //Act
            DBSource source = new DBSource()
            {
                SourceTableDefinition = source2Columns.TableDefinition,
                ConnectionManager = connection
            };
            DBDestination dest = new DBDestination()
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


        [Theory, MemberData(nameof(Connections))]
        public void WithSql(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceWithSql");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DestinationWithSql");

            //Act
            DBSource source = new DBSource()
            {
                Sql = "SELECT Col1, Col2 FROM SourceWithSql",
                ConnectionManager = connection
            };
            DBDestination dest = new DBDestination(connection, "DestinationWithSql");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithSqlNotMatchingColumns(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceNotMatchingCols");
            source2Columns.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table", @"CREATE TABLE DestinationNotMatchingCols
                (Col3 nvarchar(100) null, Col4 nvarchar(100) null, Col1 nvarchar(100) null)");

            //Act
            DBSource source = new DBSource()
            {
                Sql = "SELECT Col1, Col2 FROM SourceNotMatchingCols",
                ConnectionManager = connection
            };
            DBDestination dest = new DBDestination(connection, "DestinationNotMatchingCols");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationNotMatchingCols"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationNotMatchingCols", "Col3 = '1' AND Col4='Test1'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationNotMatchingCols", "Col3 = '2' AND Col4='Test2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationNotMatchingCols", "Col3 = '3' AND Col4='Test3'"));
        }


        [Theory, MemberData(nameof(Connections))]
        public void WithLessColumnsInDestination(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceTwoColumns");
            source2Columns.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table", @"CREATE TABLE DestinationOneColumn
                (ColX nvarchar (100) not null )");

            //Act
            DBSource source = new DBSource(connection, "SourceTwoColumns");
            DBDestination dest = new DBDestination(connection, "DestinationOneColumn");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationOneColumn"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationOneColumn", "ColX = '1'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationOneColumn", "ColX = '2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DestinationOneColumn", "ColX = '3'"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithAdditionalNotNullCol(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceAdditionalNotNullCol");
            source2Columns.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table", @"CREATE TABLE DestinationAdditionalNotNullCol
                (Col1 NVARCHAR(100) NULL, Col2 NVARCHAR(100) NULL, Col3 NVARCHAR(100) NOT NULL)");

            //Act
            DBSource source = new DBSource(connection, "SourceAdditionalNotNullCol");
            DBDestination dest = new DBDestination(connection, "DestinationAdditionalNotNullCol");
            source.LinkTo(dest);
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
