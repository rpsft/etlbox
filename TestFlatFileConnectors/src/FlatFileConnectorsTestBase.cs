using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using TestFlatFileConnectors.src.Fixture;
using TestShared.src.Helper;

namespace TestFlatFileConnectors.src
{
    [CollectionDefinition("FlatFilesToDatabase")]
    public class DataFlowCollectionClass : ICollectionFixture<FlatFileToDatabaseFixture> { }

    [Collection("FlatFilesToDatabase")]
    public class FlatFileConnectorsTestBase
    {
        protected readonly FlatFileToDatabaseFixture Fixture;

        public FlatFileConnectorsTestBase(FlatFileToDatabaseFixture fixture)
        {
            Fixture = fixture;
        }

        protected static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");
    }
}
