using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using TestShared.src.Helper;
using TestTransformations.src.Fixtures;

namespace TestTransformations.src
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
