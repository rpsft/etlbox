using ALE.ETLBox.ConnectionManager;
using TestFlatFileConnectors.Fixture;
using TestShared.Helper;

namespace TestFlatFileConnectors
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
