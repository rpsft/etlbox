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
    public class DBSourceNonGenericTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceNonGenericTests(DataFlowDatabaseFixture dbFixture)
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
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceWithSql");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DestinationWithSql");

            //Act
            DBSource source = new DBSource()
            {
                Sql = $"SELECT {s2c.QB}Col1{s2c.QE}, {s2c.QB}Col2{s2c.QE} FROM {s2c.QB}SourceWithSql{s2c.QE}",
                ConnectionManager = connection
            };
            DBDestination dest = new DBDestination(connection, "DestinationWithSql");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithSqlNotMatchingColumns(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceNotMatchingCols");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table",
                $@"CREATE TABLE destination_notmatchingcols
                ( col3 VARCHAR(100) NULL
                , col4 VARCHAR(100) NULL
                , {s2c.QB}Col1{s2c.QE} VARCHAR(100) NULL)");

            //Act
            DBSource source = new DBSource()
            {
                Sql = $"SELECT {s2c.QB}Col1{s2c.QE}, {s2c.QB}Col2{s2c.QE} FROM {s2c.QB}SourceNotMatchingCols{s2c.QE}",
                ConnectionManager = connection
            };
            DBDestination dest = new DBDestination(connection, "destination_notmatchingcols");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "destination_notmatchingcols"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_notmatchingcols", $"col3 = '1' AND col4='Test1'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_notmatchingcols", $"col3 = '2' AND col4='Test2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_notmatchingcols", $"col3 = '3' AND col4='Test3'"));
        }


        [Theory, MemberData(nameof(Connections))]
        public void WithLessColumnsInDestination(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "SourceTwoColumns");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table",
                @"CREATE TABLE destination_onecolumn
                (colx varchar (100) not null )");

            //Act
            DBSource source = new DBSource(connection, "SourceTwoColumns");
            DBDestination dest = new DBDestination(connection, "destination_onecolumn");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "destination_onecolumn"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_onecolumn", "colx = '1'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_onecolumn", "colx = '2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "destination_onecolumn", "colx = '3'"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithAdditionalNotNullCol(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "source_additionalnotnullcol");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(connection, "Create destination table", @"CREATE TABLE destination_additionalnotnullcol
                (col1 VARCHAR(100) NULL, col2 VARCHAR(100) NULL, col3 VARCHAR(100) NOT NULL)");

            //Act
            DBSource source = new DBSource(connection, "source_additionalnotnullcol");
            DBDestination dest = new DBDestination(connection, "destination_additionalnotnullcol");
            source.LinkTo(dest);
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
