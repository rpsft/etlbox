using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using TestOtherConnectors.src.Fixture;
using TestShared.src.Helper;

namespace TestOtherConnectors.src
{
    [CollectionDefinition("OtherConnectors")]
    public class OtherConnectorsCollectionClass
        : ICollectionFixture<OtherConnectorsDatabaseFixture>
    { }

    [Collection("OtherConnectors")]
    public class OtherConnectorsTestBase
    {
        protected readonly OtherConnectorsDatabaseFixture Fixture;

        public OtherConnectorsTestBase(OtherConnectorsDatabaseFixture fixture)
        {
            Fixture = fixture;
        }

        protected static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");
    }
}
