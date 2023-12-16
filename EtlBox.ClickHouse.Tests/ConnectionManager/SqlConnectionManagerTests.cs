using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.ConnectionStrings;
using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using EtlBox.Database.Tests.Infrastructure;
using Microsoft.Data.SqlClient;
using TestShared.src.Attributes;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ConnectionManager
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class SqlConnectionManagerTests : DatabaseTestBase
    {
        protected SqlConnectionManagerTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            CreateDatabase(Guid.NewGuid().ToString());
        }

        [Fact]
        public void TestOpeningCloseConnection()
        {
            //Arrange
            var con = GetConnectionManager();
            con.LeaveOpen = true;

            //Act
            AssertOpenConnectionCount(0);
            con.Open();
            AssertOpenConnectionCount(1);
            con.Close(); //won't close any connection - ado.net will keep the connection open in it's pool in case it's needed again
            AssertOpenConnectionCount(1);
            SqlConnection.ClearAllPools();

            //Assert
            AssertOpenConnectionCount(0);
        }

        [Fact]
        public void TestOpeningConnectionTwice()
        {
            var con = GetConnectionManager();
            AssertOpenConnectionCount(0);
            con.Open();
            con.Open();
            AssertOpenConnectionCount(1);
            con.Close();
            AssertOpenConnectionCount(1);
            SqlConnection.ClearAllPools();
            AssertOpenConnectionCount(0);
        }

        [MultiprocessorOnlyFact(Skip = "TODO: Hangs on Apple silicon and Docker")]
        public void TestOpeningConnectionsParallelOnSqlTask()
        {
            AssertOpenConnectionCount(0);
            var array = new List<int> { 1, 2, 3, 4 };
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
                        ConnectionManager = new SqlConnectionManager(
                            new SqlConnectionString(_connectionString)
                        ),
                        DisableLogging = true
                    }.ExecuteNonQuery()
            );
            AssertOpenConnectionCount(2);
            SqlConnection.ClearAllPools();
            AssertOpenConnectionCount(0);
        }

        [Fact]
        public void TestCloningConnection()
        {
            //Arrange
            var con = GetConnectionManager();

            //Act
            var clone = con.Clone();

            //Assert
            Assert.NotEqual(clone, con);
        }

        private void AssertOpenConnectionCount(
            int allowedOpenConnections
        )
        {
            var conString = new SqlConnectionString(_connectionString);
            var master = new SqlConnectionManager(conString.CloneWithMasterDbName());
            var dbName = conString.Builder.InitialCatalog;
            var openConnections = new SqlTask(
                "Count open connections",
                $@"SELECT COUNT(dbid) as NumberOfConnections FROM sys.sysprocesses
                    WHERE dbid > 0 and DB_NAME(dbid) = '{dbName}'"
            )
            {
                ConnectionManager = master,
                DisableLogging = true
            }.ExecuteScalar<int>();
            Assert.Equal(allowedOpenConnections, openConnections);
        }

        private IConnectionManager GetConnectionManager()
            => _fixture.GetContainer(_connectionType).GetConnectionManager();

        private void CreateDatabase(string database)
            => _fixture.GetContainer(_connectionType).CreateDatabase(database);

        private string _connectionString
            => _fixture.GetContainer(_connectionType).GetConnectionString();

        public class SqlServer : SqlConnectionManagerTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }
    }
}
