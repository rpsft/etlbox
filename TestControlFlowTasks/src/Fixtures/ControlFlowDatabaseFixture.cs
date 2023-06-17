using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace TestControlFlowTasks.Fixtures
{
    [CollectionDefinition("ControlFlow")]
    public class ControlFlowCollectionClass : ICollectionFixture<ControlFlowDatabaseFixture> { }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
    public sealed class ControlFlowDatabaseFixture : IDisposable
    {
        private readonly string _sqliteFilePath;
        public const string ConfigSection = "ControlFlow";

        public ControlFlowDatabaseFixture()
        {
            _sqliteFilePath = Config.SQLiteConnection.ConnectionString(ConfigSection).DbName;
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, ConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, ConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, ConfigSection);
            File.Copy(_sqliteFilePath, _sqliteFilePath + ".bak");
        }

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.SqlConnection, ConfigSection);
            DatabaseHelper.DropDatabase(Config.MySqlConnection, ConfigSection);
            DatabaseHelper.DropDatabase(Config.PostgresConnection, ConfigSection);
            File.Delete(_sqliteFilePath);
            File.Move(_sqliteFilePath + ".bak", _sqliteFilePath);
        }
    }
}
