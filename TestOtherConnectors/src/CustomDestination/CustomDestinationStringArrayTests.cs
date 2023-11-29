using ALE.ETLBox.Common;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.ControlFlow;
using TestShared.SharedFixtures;

namespace TestOtherConnectors.CustomDestination
{
    public class CustomDestinationStringArrayTests : OtherConnectorsTestBase
    {
        public CustomDestinationStringArrayTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void InsertIntoTable()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "CustomDestinationNonGenericSource"
            );
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "CustomDestinationNonGenericDestination"
            );

            //Act
            DbSource<string[]> source = new DbSource<string[]>(
                SqlConnection,
                "CustomDestinationNonGenericSource"
            );
            CustomDestination<string[]> dest = new CustomDestination<string[]>(row =>
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
