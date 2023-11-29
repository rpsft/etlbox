using System.Dynamic;
using ALE.ETLBox.Common;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.ControlFlow;
using TestShared.SharedFixtures;

namespace TestOtherConnectors.CustomDestination
{
    public class CustomDestinationDynamicObjectTests : OtherConnectorsTestBase
    {
        public CustomDestinationDynamicObjectTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void InsertIntoTable()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "CustomDestinationDynamicSource"
            );
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "CustomDestinationDynamicDestination"
            );

            //Act
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(
                SqlConnection,
                "CustomDestinationDynamicSource"
            );
            CustomDestination<ExpandoObject> dest = new CustomDestination<ExpandoObject>(row =>
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
