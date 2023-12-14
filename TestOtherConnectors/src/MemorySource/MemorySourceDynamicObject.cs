using System.Dynamic;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.MemorySource
{
    public class MemorySourceDynamicObjectTests : OtherConnectorsTestBase
    {
        public MemorySourceDynamicObjectTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void DataIsFromList()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture("MemoryDestination");
            var source = new MemorySource<ExpandoObject>();
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "MemoryDestination"
            );
            AddObjectsToSource(source);

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        private static void AddObjectsToSource(MemorySource<ExpandoObject> source)
        {
            source.DataAsList = new List<ExpandoObject>();
            dynamic item1 = new ExpandoObject();
            item1.Col2 = "Test1";
            item1.Col1 = 1;
            dynamic item2 = new ExpandoObject();
            item2.Col2 = "Test2";
            item2.Col1 = 2;
            dynamic item3 = new ExpandoObject();
            item3.Col2 = "Test3";
            item3.Col1 = 3;
            source.DataAsList.Add(item1);
            source.DataAsList.Add(item2);
            source.DataAsList.Add(item3);
        }
    }
}
