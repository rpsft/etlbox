using ALE.ETLBox.ConnectionManager;
using ETLBox.Primitives;
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

        public static TheoryData<IConnectionManager> AllSqlConnections =>
            new(Config.AllSqlConnections("DataFlow"));
    }
}
