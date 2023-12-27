using ALE.ETLBox.ConnectionManager;
using ClickHouse.Ado;
using EtlBox.ClickHouse.ConnectionManager;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class ControlFlowTestBase
    {
        private static string ConfigSection => ControlFlowDatabaseFixture.ConfigSection;

        protected ControlFlowTestBase(ControlFlowDatabaseFixture fixture) { }

        protected static MySqlConnectionManager MySqlConnection =>
            Config.MySqlConnection.ConnectionManager(ConfigSection);

        protected static PostgresConnectionManager PostgresConnection =>
            Config.PostgresConnection.ConnectionManager(ConfigSection);

        public static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager(ConfigSection);

        public static ClickHouseConnectionManager ClickHouseConnection =>
            Config.ClickHouseConnection.ConnectionManager(ConfigSection);

        public static AdomdConnectionManager AdomdConnection =>
            new(Config.SSASConnection.ConnectionString(ConfigSection).CloneWithoutDbName());

        protected static SQLiteConnectionManager SqliteConnection =>
            Config.SQLiteConnection.ConnectionManager(ConfigSection);

        public static IEnumerable<object[]> AccessConnection =>
            Config.AccessConnection(ConfigSection);

        public static IEnumerable<object[]> AllSqlConnections =>
            Config.AllSqlConnections(ConfigSection);

        public static IEnumerable<object[]> AllConnectionsWithoutSQLite =>
            Config.AllConnectionsWithoutSQLite(ConfigSection);

        public static IEnumerable<object[]> AllConnectionsWithoutClickHouse =>
            Config.AllConnectionsWithoutClickHouse(ConfigSection);

        public static IEnumerable<object[]> AllConnectionsWithoutSQLiteAndClickHouse =>
            Config.AllConnectionsWithoutSQLiteAndClickHouse(ConfigSection);

        public static IEnumerable<object[]> DbConnectionsWithMaster() =>
            new[]
            {
                new object[]
                {
                    new ClickHouseConnectionManager(
                        Config.ClickHouseConnection
                            .ConnectionString(ConfigSection)
                            .CloneWithMasterDbName()
                    )
                },
                new object[]
                {
                    new SqlConnectionManager(
                        Config.SqlConnection.ConnectionString(ConfigSection).CloneWithMasterDbName()
                    )
                },
                new object[]
                {
                    new PostgresConnectionManager(
                        Config.PostgresConnection
                            .ConnectionString(ConfigSection)
                            .CloneWithMasterDbName()
                    )
                },
                new object[]
                {
                    new MySqlConnectionManager(
                        Config.MySqlConnection
                            .ConnectionString(ConfigSection)
                            .CloneWithMasterDbName()
                    )
                },
            };
    }
}
