using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationStringArrayTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbDestinationStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithSqlNotMatchingColumns(IConnectionManager conn)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(conn, "SourceNotMatchingCols");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(conn, "Create destination table",
                $@"CREATE TABLE {conn.QB}destination_notmatchingcols{conn.QE}
                ( {conn.QB}col3{conn.QE} VARCHAR(100) NULL
                , {conn.QB}col4{conn.QE} VARCHAR(100) NULL
                , {conn.QB}Col1{conn.QE} VARCHAR(100) NULL)");

            //Act
            DbSource<string[]> source = new DbSource<string[]>()
            {
                Sql = $"SELECT {s2c.QB}Col1{s2c.QE}, {s2c.QB}Col2{s2c.QE} FROM {s2c.QB}SourceNotMatchingCols{s2c.QE}",
                ConnectionManager = conn
            };
            DbDestination<string[]> dest = new DbDestination<string[]>(conn, "destination_notmatchingcols");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(conn, "destination_notmatchingcols"));
            Assert.Equal(1, RowCountTask.Count(conn, "destination_notmatchingcols", $"{conn.QB}col3{conn.QE} = '1' AND {conn.QB}col4{conn.QE}='Test1'"));
            Assert.Equal(1, RowCountTask.Count(conn, "destination_notmatchingcols", $"{conn.QB}col3{conn.QE} = '2' AND {conn.QB}col4{conn.QE}='Test2'"));
            Assert.Equal(1, RowCountTask.Count(conn, "destination_notmatchingcols", $"{conn.QB}col3{conn.QE} = '3' AND {conn.QB}col4{conn.QE}='Test3'"));
        }


        [Theory, MemberData(nameof(Connections))]
        public void WithLessColumnsInDestination(IConnectionManager conn)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(conn, "SourceTwoColumns");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(conn, "Create destination table",
                $@"CREATE TABLE {conn.QB}destination_onecolumn{conn.QE}
                ({conn.QB}colx{conn.QE} varchar (100) not null )");

            //Act
            DbSource<string[]> source = new DbSource<string[]>(conn, "SourceTwoColumns");
            DbDestination<string[]> dest = new DbDestination<string[]>(conn, "destination_onecolumn");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(conn, "destination_onecolumn"));
            Assert.Equal(1, RowCountTask.Count(conn, "destination_onecolumn", $"{conn.QB}colx{conn.QE} = '1'"));
            Assert.Equal(1, RowCountTask.Count(conn, "destination_onecolumn", $"{conn.QB}colx{conn.QE} = '2'"));
            Assert.Equal(1, RowCountTask.Count(conn, "destination_onecolumn", $"{conn.QB}colx{conn.QE} = '3'"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithAdditionalNullableCol(IConnectionManager conn)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(conn, "source_additionalnullcol");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(conn, "Create destination table",
                $@"CREATE TABLE {conn.QB}destination_additionalnullcol{conn.QE}
                ( {conn.QB}col1{conn.QE} VARCHAR(100) NULL
                , {conn.QB}col2{conn.QE} VARCHAR(100) NULL
                , {conn.QB}col3{conn.QE} VARCHAR(100) NULL)");

            //Act
            DbSource<string[]> source = new DbSource<string[]>(conn, "source_additionalnullcol");
            DbDestination<string[]> dest = new DbDestination<string[]>(conn, "destination_additionalnullcol");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            s2c.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithAdditionalNotNullCol(IConnectionManager conn)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(conn, "source_additionalnotnullcol");
            s2c.InsertTestData();
            SqlTask.ExecuteNonQuery(conn, "Create destination table",
                $@"CREATE TABLE {conn.QB}destination_additionalnotnullcol{conn.QE}
                ( {conn.QB}col1{conn.QE} VARCHAR(100) NULL
                , {conn.QB}col2{conn.QE} VARCHAR(100) NULL
                , {conn.QB}col3{conn.QE} VARCHAR(100) NOT NULL)");

            //Act
            DbSource<string[]> source = new DbSource<string[]>(conn, "source_additionalnotnullcol");
            DbDestination<string[]> dest = new DbDestination<string[]>(conn, "destination_additionalnotnullcol");
            source.LinkTo(dest);
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
