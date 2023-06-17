using System.IO;
using TestShared.Helper;

namespace TestDatabaseConnectors.Fixtures
{
    public sealed class DatabaseSourceDestinationFixture : IDisposable
    {
        private readonly string _sqliteFilePath;
        private string SqliteBackupFileName => _sqliteFilePath + ".bak";
        public const string SourceConfigSection = "DataFlowSource";
        public const string DestinationConfigSection = "DataFlowDestination";
        public const string OtherConfigSection = "Other";

        public DatabaseSourceDestinationFixture()
        {
            _sqliteFilePath = Config.SQLiteConnection.ConnectionString(SourceConfigSection).DbName;
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, DestinationConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, DestinationConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, DestinationConfigSection);
            if (!File.Exists(SqliteBackupFileName))
                File.Copy(_sqliteFilePath, SqliteBackupFileName);
        }

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.SqlConnection, SourceConfigSection);
            DatabaseHelper.DropDatabase(Config.SqlConnection, DestinationConfigSection);
            DatabaseHelper.DropDatabase(Config.MySqlConnection, SourceConfigSection);
            DatabaseHelper.DropDatabase(Config.MySqlConnection, DestinationConfigSection);
            DatabaseHelper.DropDatabase(Config.PostgresConnection, SourceConfigSection);
            DatabaseHelper.DropDatabase(Config.PostgresConnection, DestinationConfigSection);
            File.Delete(_sqliteFilePath);
            File.Move(SqliteBackupFileName, _sqliteFilePath);
        }
    }
}
