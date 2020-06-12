using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using Xunit;
using static ETLBoxTests.Helper.Config;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class GenericOdbcConnectionManagerTests
    {

        public static ConnectionDetails<OdbcConnectionString, OdbcConnectionManager> SqlGenericOdbcConnectionDetails
        { get; set; } = new ConnectionDetails<OdbcConnectionString, OdbcConnectionManager>("SqlOdbcConnectionString");

        public static ConnectionDetails<OdbcConnectionString, OdbcConnectionManager> MySqlGenericOdbcConnectionDetails
        { get; set; } = new ConnectionDetails<OdbcConnectionString, OdbcConnectionManager>("MySqlOdbcConnectionString");
        public static ConnectionDetails<OdbcConnectionString, OdbcConnectionManager> PostgresGenericOdbcConnectionDetails
        { get; set; } = new ConnectionDetails<OdbcConnectionString, OdbcConnectionManager>("PostgresOdbcConnectionString");

        public static IEnumerable<object[]> OdbcGenericConnections() => new[] {
                    new object[] { (IConnectionManager)SqlGenericOdbcConnectionDetails.ConnectionManager("DataFlow") },
                    new object[] { (IConnectionManager)MySqlGenericOdbcConnectionDetails.ConnectionManager("DataFlow") },
                    new object[] { (IConnectionManager)PostgresGenericOdbcConnectionDetails.ConnectionManager("DataFlow") }
        };

        public GenericOdbcConnectionManagerTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public long Col1 { get; set; }
            public string Col2 { get; set; }
        }

        private TableDefinition _sourcedef;
        private TableDefinition _destdef;

        private void CreateSourceAndDestinationTables(IConnectionManager connection, string sourcetable, string desttable)
        {
            var cols = new List<TableColumn>() {
                new TableColumn("Col1", "INT", allowNulls: false),
                new TableColumn("Col2", "VARCHAR(100)", allowNulls: true)
            };
            _sourcedef = new TableDefinition(sourcetable, cols);
            _destdef = new TableDefinition(desttable, cols);
            CreateTableTask.Create(connection, _sourcedef);
            CreateTableTask.Create(connection, _destdef);
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
            , $@"INSERT INTO {sourcetable} VALUES(1,'Test1')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                , $@"INSERT INTO {sourcetable} VALUES(2,'Test2')");
            SqlTask.ExecuteNonQuery(connection, "Insert demo data"
                 , $@"INSERT INTO {sourcetable} VALUES(3,'Test3')");
        }


        [Theory, MemberData(nameof(OdbcGenericConnections))]
        public void WithTableName(IConnectionManager connection)
        {
            //Arrange
            CreateSourceAndDestinationTables(connection, "dbsource_genericodbc_tn", "dbdest_genericodbc_tn");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection, "dbdest_genericodbc_tn");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection, "dbdest_genericodbc_tn");

            Assert.ThrowsAny<Exception>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }


        [Theory, MemberData(nameof(OdbcGenericConnections))]
        public void WithTableDefinition(IConnectionManager connection)
        {
            //Arrange
            //Arrange
            CreateSourceAndDestinationTables(connection, "dbsource_genericodbc_def", "dbdest_genericodbc_def");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(connection)
            {
                SourceTableDefinition = _sourcedef
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(connection)
            {
                DestinationTableDefinition = _destdef
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.Equal(3, RowCountTask.Count(connection, "dbdest_genericodbc_def"));
            Assert.Equal(1, RowCountTask.Count(connection, "dbdest_genericodbc_def", $"col1 = 1 AND col2='Test1'"));
            Assert.Equal(1, RowCountTask.Count(connection, "dbdest_genericodbc_def", $"col1 = 2 AND col2='Test2'"));
            Assert.Equal(1, RowCountTask.Count(connection, "dbdest_genericodbc_def", $"col1 = 3 AND col2='Test3'"));
        }
    }
}
