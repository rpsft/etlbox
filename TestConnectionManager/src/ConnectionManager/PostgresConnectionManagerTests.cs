using System.Data;
using System.Dynamic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using Npgsql;
using TestConnectionManager.Fixtures;

namespace TestConnectionManager.ConnectionManager
{
    [Collection("Connection Manager")]
    public class PostgresConnectionManagerTests : ConnectionManagerTestBase
    {
        public PostgresConnectionManagerTests(ConnectionManagerFixture fixture)
            : base(fixture) { }

        private static int? GetOpenConnections(string connectionString)
        {
            var conString = new PostgresConnectionString(connectionString);
            var master = new PostgresConnectionManager(conString.CloneWithMasterDbName());
            var dbName = conString.Builder.Database;
            int? openConnections = new SqlTask(
                "Count open connections", $@"
                SELECT count(*)
                FROM pg_stat_activity
                where datname = '{dbName}'"
            )
            {
                ConnectionManager = master,
                DisableLogging = true
            }.ExecuteScalar<int>();
            return openConnections;
        }

        [Fact]
        public void TestLeaveConnectionOpen()
        {
            Assert.Equal(0, GetOpenConnections(PostgresConnectionStringParameter));
            //Arrange
            var con = new PostgresConnectionManager(PostgresConnectionStringParameter) { LeaveOpen = true };

            //Act
            Assert.Equal(0, GetOpenConnections(PostgresConnectionStringParameter) - 0);
            Assert.True(con.State == null);
            con.Open();
            Assert.True(con.State == ConnectionState.Open);
            Assert.Equal(1, GetOpenConnections(PostgresConnectionStringParameter) - 0);
            con.Open();
            Assert.Equal(1, GetOpenConnections(PostgresConnectionStringParameter) - 0);
            Assert.True(con.State == ConnectionState.Open);
            NpgsqlConnection.ClearAllPools();

            //Assert
            Assert.Equal(1, GetOpenConnections(PostgresConnectionStringParameter) - 0);
            con.Close();
            NpgsqlConnection.ClearAllPools();
            Assert.Equal(0, GetOpenConnections(PostgresConnectionStringParameter) - 0);
        }

        [Fact]
        public void TestLeaveConnectionOpenWithSqlTask()
        {
            //Arrange
            var initialConnections = GetOpenConnections(PostgresConnectionStringParameter);
            var con = new PostgresConnectionManager(PostgresConnectionStringParameter) { LeaveOpen = true };

            //Act
            Assert.Equal(0, GetOpenConnections(PostgresConnectionStringParameter) - initialConnections);
            Assert.True(con.State == null);
            SqlTask.ExecuteNonQuery(con, "Dummy", "SELECT 1");
            Assert.True(con.State == ConnectionState.Open);
            Assert.Equal(1, GetOpenConnections(PostgresConnectionStringParameter) - initialConnections);
            SqlTask.ExecuteNonQuery(con, "Dummy", "SELECT 1");
            Assert.Equal(1, GetOpenConnections(PostgresConnectionStringParameter) - initialConnections);
            Assert.True(con.State == ConnectionState.Open);
            Assert.Equal(1, GetOpenConnections(PostgresConnectionStringParameter) - initialConnections);
            NpgsqlConnection.ClearAllPools();

            //Assert
            Assert.Equal(1, GetOpenConnections(PostgresConnectionStringParameter) - initialConnections);
            con.Close();
            NpgsqlConnection.ClearAllPools();
            Assert.Equal(0, GetOpenConnections(PostgresConnectionStringParameter) - initialConnections);
        }

        [Fact]
        public void TestCloningIfAllowedConnection()
        {
            //Arrange
            var con = new PostgresConnectionManager(PostgresConnectionStringParameter) { LeaveOpen = true };

            //Act
            var cloneIfAllowed = con.CloneIfAllowed();
            var clone = con.Clone();

            //Assert
            Assert.Equal(cloneIfAllowed, con);
            Assert.NotEqual(clone, con);
        }

        [Fact]
        public void TestSimpleQueryForNotLeakingConnections()
        {
            //Arrange
            var initialConnectionCount = GetOpenConnections(PostgresConnectionStringParameter);
            var con = new PostgresConnectionManager(PostgresConnectionStringParameter);

            //Act
            new SqlTask("DUMMY", "select 1")
            { 
                ConnectionManager = con
            }.ExecuteScalar();

            //ASsert
            NpgsqlConnection.ClearAllPools();
            var connectionCount = GetOpenConnections(PostgresConnectionStringParameter);
            Assert.Equal(0, connectionCount - initialConnectionCount);
        }

        [Fact]
        public void TestBulkInsertForNotLeakingConnections()
        {
            //Arrange
            var con = new PostgresConnectionManager(PostgresConnectionStringParameter);

            var tableName = "BulkInsertTest";
            TableDefinition tableDef = PrepareBulkInsert(con, tableName);

            var data = new TableData<string[]>(tableDef);
            object[] values = { "1", "Test1" };
            data.Rows.Add(values);

            NpgsqlConnection.ClearAllPools();
            var initialConnectionCount = GetOpenConnections(PostgresConnectionStringParameter);

            //Act
            SqlTask.BulkInsert(con, "BI", data, tableName);

            //ASsert
            NpgsqlConnection.ClearAllPools();
            var connectionCount = GetOpenConnections(PostgresConnectionStringParameter);
            Assert.Equal(0, connectionCount - initialConnectionCount);
        }

        [Fact]
        public void TestDbDestinationForNotLeakingConnections()
        {
            //Arrange
            var con = new PostgresConnectionManager(PostgresConnectionStringParameter);

            var tableName = "BulkInsertTest";
            PrepareBulkInsert(con, tableName);

            var data = new ExpandoObject();
            var dict = data as IDictionary<string, object>;
            dict["Col1"] = 1;
            dict["Col2"] = "Test1";

            NpgsqlConnection.ClearAllPools();
            var initialConnectionCount = GetOpenConnections(PostgresConnectionStringParameter);

            //Act
            var source = new MemorySource(new[] { data });
            var dest = new DbDestination(con, tableName);
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            //Assert
            NpgsqlConnection.ClearAllPools();
            var connectionCount = GetOpenConnections(PostgresConnectionStringParameter);
            Assert.Equal(0, connectionCount - initialConnectionCount);
        }

        [Fact]
        public void TestDbRowTransformationForNotLeakingConnections()
        {
            //Arrange
            var con = new PostgresConnectionManager(PostgresConnectionStringParameter);

            var tableName = "BulkInsertTest";
            PrepareBulkInsert(con, tableName);

            var data = new ExpandoObject();
            var dict = data as IDictionary<string, object>;
            dict["Col1"] = 1;
            dict["Col2"] = "Test1";

            NpgsqlConnection.ClearAllPools();
            var initialConnectionCount = GetOpenConnections(PostgresConnectionStringParameter);

            //Act
            var source = new MemorySource(new[] { data });
            var transformation = new DbRowTransformation<ExpandoObject>(con, tableName);
            var dest = new MemoryDestination();
            source.LinkTo(transformation);
            transformation.LinkTo(dest);

            source.Execute();
            dest.Wait();

            //Assert
            NpgsqlConnection.ClearAllPools();
            var connectionCount = GetOpenConnections(PostgresConnectionStringParameter);
            Assert.Equal(0, connectionCount - initialConnectionCount);
        }

        private static TableDefinition PrepareBulkInsert(PostgresConnectionManager con, string tableName)
        {
            DropTableTask.DropIfExists(con, tableName);

            var tableDef = new TableDefinition(
                tableName,
                new List<TableColumn>
                {
                    new("Col1", "INT", allowNulls: false, isPrimaryKey: true),
                    new("Col2", "VARCHAR(100)", allowNulls: true)
                }
            );
            tableDef.CreateTable(con);
            return tableDef;
        }
    }
}
