using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.CsvDestination
{
    public class ACsvDestinationDynamicObjectTests : FlatFileConnectorsTestBase
    {
        public ACsvDestinationDynamicObjectTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("CSVDestDynamicObject");
            s2C.InsertTestDataSet3();
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(
                SqlConnection,
                "CSVDestDynamicObject"
            );

            //Act
            CsvDestination<ExpandoObject> dest = new CsvDestination<ExpandoObject>(
                "./SimpleWithDynamicObject.csv"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("./SimpleWithDynamicObject.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsSet3DynamicObject.csv")
            );
        }
    }
}
