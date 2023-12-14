using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.CustomDestination
{
    public class CustomDestinationStringArrayTests : OtherConnectorsTestBase
    {
        public CustomDestinationStringArrayTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void InsertIntoTable()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                "CustomDestinationNonGenericSource"
            );
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(
                "CustomDestinationNonGenericDestination"
            );

            //Act
            var source = new DbSource<string[]>(
                SqlConnection,
                "CustomDestinationNonGenericSource"
            );
            var dest = new CustomDestination<string[]>(row =>
            {
                SqlTask.ExecuteNonQuery(
                    SqlConnection,
                    "Insert row",
                    $"INSERT INTO dbo.CustomDestinationNonGenericDestination VALUES({row[0]},'{row[1]}')"
                );
            });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
