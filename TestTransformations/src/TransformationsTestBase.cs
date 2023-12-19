using ALE.ETLBox.ConnectionManager;
using TestShared.Helper;
using TestTransformations.Fixtures;

namespace TestTransformations
{
    [CollectionDefinition("Transformations")]
    public class DataFlowCollection : ICollectionFixture<TransformationsDatabaseFixture> { }

    [Collection("Transformations")]
    public class TransformationsTestBase
    {
        protected readonly TransformationsDatabaseFixture Fixture;

        protected TransformationsTestBase(TransformationsDatabaseFixture fixture)
        {
            Fixture = fixture;
        }

        protected static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public static IEnumerable<object[]> AllSqlConnections =>
            Config.AllSqlConnections("DataFlow");
    }
}
