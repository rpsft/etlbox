using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Dynamic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CustomDestinationDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CustomDestinationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void InsertIntoTable()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("CustomDestinationDynamicSource");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CustomDestinationDynamicDestination");

            //Act
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(SqlConnection, "CustomDestinationDynamicSource");
            CustomDestination<ExpandoObject> dest = new CustomDestination<ExpandoObject>(
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    SqlTask.ExecuteNonQuery(SqlConnection, "Insert row",
                        $"INSERT INTO dbo.CustomDestinationDynamicDestination VALUES({r.Col1},'{r.Col2}')");
                }
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
