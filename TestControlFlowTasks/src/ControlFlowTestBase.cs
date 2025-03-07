using ALE.ETLBox.ConnectionManager;
using ETLBox.ClickHouse.ConnectionManager;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    [Collection(nameof(ControlFlowCollection))]
    public class ControlFlowTestBase
    {
        private readonly ControlFlowDatabaseFixture _fixture;
        private static string ConfigSection => ControlFlowDatabaseFixture.ConfigSection;

        protected ControlFlowTestBase(ControlFlowDatabaseFixture fixture)
        {
            _fixture = fixture;
        }

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

        protected SQLiteConnectionManager SqliteConnection =>
            Config.SQLiteConnection.ConnectionManager(ConfigSection, _fixture.SQLiteDbSuffix);

        public static TheoryData<IConnectionManager> AccessConnection =>
            new(Config.AccessConnection(ConfigSection));

        public static TheoryData<IConnectionManager> AllSqlConnections =>
            new(Config.AllSqlConnections(ConfigSection));

        public static TheoryData<IConnectionManager> AllConnectionsWithoutSQLite =>
            new(Config.AllConnectionsWithoutSQLite(ConfigSection));

        public static TheoryData<IConnectionManager> AllConnectionsWithoutClickHouse =>
            new(Config.AllConnectionsWithoutClickHouse(ConfigSection));

        public static TheoryData<IConnectionManager> AllConnectionsWithoutSQLiteAndClickHouse =>
            new(Config.AllConnectionsWithoutSQLiteAndClickHouse(ConfigSection));

        public static TheoryData<IConnectionManager> DbConnectionsWithMaster() =>
            [
                new ClickHouseConnectionManager(
                    Config
                        .ClickHouseConnection.ConnectionString(ConfigSection)
                        .CloneWithMasterDbName()
                ),
                new SqlConnectionManager(
                    Config.SqlConnection.ConnectionString(ConfigSection).CloneWithMasterDbName()
                ),
                new PostgresConnectionManager(
                    Config
                        .PostgresConnection.ConnectionString(ConfigSection)
                        .CloneWithMasterDbName()
                ),
                new MySqlConnectionManager(
                    Config.MySqlConnection.ConnectionString(ConfigSection).CloneWithMasterDbName()
                ),
            ];
    }
}
