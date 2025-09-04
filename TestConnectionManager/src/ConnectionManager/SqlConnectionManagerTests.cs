using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using Microsoft.Data.SqlClient;
using TestConnectionManager.Fixtures;
using TestShared.Attributes;

namespace TestConnectionManager.ConnectionManager
{
    [Collection("Connection Manager")]
    public sealed class SqlConnectionManagerTests : ConnectionManagerTestBase
    {
        public SqlConnectionManagerTests(ConnectionManagerFixture fixture)
            : base(ETLBox.Primitives.ConnectionManagerType.SqlServer, fixture) { }

        private static void AssertOpenConnectionCount(
            int allowedOpenConnections,
            string connectionString
        )
        {
            var conString = new SqlConnectionString(connectionString);
            using var master = new SqlConnectionManager(conString.CloneWithMasterDbName());
            var dbName = conString.Builder.InitialCatalog;
            var openConnections = new SqlTask(
                "Count open connections",
                $@"SELECT COUNT(dbid) as NumberOfConnections FROM sys.sysprocesses
                    WHERE dbid > 0 and DB_NAME(dbid) = '{dbName}'"
            )
            {
                ConnectionManager = master,
                DisableLogging = true,
            }.ExecuteScalar<int>();
            Assert.Equal(allowedOpenConnections, openConnections);
        }

        [Fact]
        public void TestOpeningCloseConnection()
        {
            //Arrange
            using var con = new SqlConnectionManager(
                new SqlConnectionString(ConnectionStringParameter)
            );

            //Act
            AssertOpenConnectionCount(0, ConnectionStringParameter);
            con.Open();
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            con.Close(); //won't close any connection - ado.net will keep the connection open in it's pool in case it's needed again
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            SqlConnection.ClearAllPools();

            //Assert
            AssertOpenConnectionCount(0, ConnectionStringParameter);
        }

        [Fact]
        public void TestOpeningConnectionTwice()
        {
            using var con = new SqlConnectionManager(
                new SqlConnectionString(ConnectionStringParameter)
            );
            AssertOpenConnectionCount(0, ConnectionStringParameter);
            con.Open();
            con.Open();
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            con.Close();
            AssertOpenConnectionCount(1, ConnectionStringParameter);
            SqlConnection.ClearAllPools();
            AssertOpenConnectionCount(0, ConnectionStringParameter);
        }

        [MultiprocessorOnlyFact(Skip = "TODO: Hangs on Apple silicon and Docker")]
        public void TestOpeningConnectionsParallelOnSqlTask()
        {
            AssertOpenConnectionCount(0, ConnectionStringParameter);
            var array = new List<int> { 1, 2, 3, 4 };
            var manager = new SqlConnectionManager(
                new SqlConnectionString(ConnectionStringParameter)
            );
            Parallel.ForEach(
                array,
                new ParallelOptions { MaxDegreeOfParallelism = 2 },
                curNr =>
                    new SqlTask(
                        $"Test statement {curNr}",
                        $@"
                    DECLARE @counter INT = 0;
                    CREATE TABLE dbo.test{curNr} (
                        Col1 nvarchar(50)
                    )
                    WHILE @counter <= 10000
                    BEGIN
                        SET @counter = @counter + 1;
                         INSERT INTO dbo.test{curNr}
                            values('Lorem ipsum Lorem ipsum Lorem ipsum Lorem')
                    END
            "
                    )
                    {
                        ConnectionManager = manager,
                        DisableLogging = true,
                    }.ExecuteNonQuery()
            );
            AssertOpenConnectionCount(2, ConnectionStringParameter);
            SqlConnection.ClearAllPools();
            AssertOpenConnectionCount(0, ConnectionStringParameter);

            manager?.Dispose();
        }

        [Fact]
        public void TestCloningConnection()
        {
            //Arrange
            using var con = new SqlConnectionManager(ConnectionStringParameter);

            //Act
            using var clone = con.Clone() as SqlConnectionManager;

            //Assert
            Assert.NotEqual(clone, con);
        }
    }
}
