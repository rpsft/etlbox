using System.Diagnostics.CodeAnalysis;

namespace TestControlFlowTasks.Fixtures
{
    [CollectionDefinition("ControlFlow")]
    public class ControlFlowCollectionClass : ICollectionFixture<ControlFlowDatabaseFixture> { }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
    public class ControlFlowDatabaseFixture
    {
        public ControlFlowDatabaseFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("ControlFlow");
            DatabaseHelper.RecreateMySqlDatabase("ControlFlow");
            DatabaseHelper.RecreatePostgresDatabase("ControlFlow");
        }
    }
}
