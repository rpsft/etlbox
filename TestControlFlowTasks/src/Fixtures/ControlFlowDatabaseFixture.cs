using System.Diagnostics.CodeAnalysis;

namespace TestControlFlowTasks.Fixtures
{
    [CollectionDefinition("ControlFlow")]
    public class ControlFlowCollectionClass : ICollectionFixture<ControlFlowDatabaseFixture> { }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
    public sealed class ControlFlowDatabaseFixture : IDisposable
    {
        public const string ConfigSection = "ControlFlow";

        public ControlFlowDatabaseFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, ConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, ConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, ConfigSection);
        }

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.SqlConnection, ConfigSection);
            DatabaseHelper.DropDatabase(Config.MySqlConnection, ConfigSection);
            DatabaseHelper.DropDatabase(Config.PostgresConnection, ConfigSection);
        }
    }
}
