using System.Diagnostics.CodeAnalysis;
using System.IO;
using TestShared.Helper;

namespace TestControlFlowTasks.Fixtures
{
    [CollectionDefinition("ControlFlow")]
    public class ControlFlowCollectionClass : ICollectionFixture<ControlFlowDatabaseFixture> { }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
    public sealed class ControlFlowDatabaseFixture : IDisposable
    {
        private readonly string _sqliteFilePath;
        private string SqliteBackupFileName => _sqliteFilePath + ".bak";
        public const string ConfigSection = "ControlFlow";

        public ControlFlowDatabaseFixture()
        {
            _sqliteFilePath = Config.SQLiteConnection.ConnectionString(ConfigSection).DbName;
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, ConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, ConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, ConfigSection);
            if (!File.Exists(SqliteBackupFileName))
                File.Copy(_sqliteFilePath, SqliteBackupFileName);
        }

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.SqlConnection, ConfigSection);
            DatabaseHelper.DropDatabase(Config.MySqlConnection, ConfigSection);
            DatabaseHelper.DropDatabase(Config.PostgresConnection, ConfigSection);
            File.Delete(_sqliteFilePath);
            File.Move(SqliteBackupFileName, _sqliteFilePath);
        }
    }
}
