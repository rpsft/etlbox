using System.Dynamic;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.CustomDestination
{
    public class CustomDestinationDynamicObjectTests : OtherConnectorsTestBase
    {
        public CustomDestinationDynamicObjectTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void InsertIntoTable()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                "CustomDestinationDynamicSource"
            );
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(
                "CustomDestinationDynamicDestination"
            );

            //Act
            var source = new DbSource<ExpandoObject>(
                SqlConnection,
                "CustomDestinationDynamicSource"
            );
            var dest = new CustomDestination<ExpandoObject>(row =>
            {
                dynamic r = row;
                SqlTask.ExecuteNonQuery(
                    SqlConnection,
                    "Insert row",
                    $"INSERT INTO dbo.CustomDestinationDynamicDestination VALUES({r.Col1},'{r.Col2}')"
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
