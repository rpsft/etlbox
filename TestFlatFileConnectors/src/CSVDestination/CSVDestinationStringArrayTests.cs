using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.CSVDestination
{
    public class CsvDestinationStringArrayTests : FlatFileConnectorsTestBase
    {
        public CsvDestinationStringArrayTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleNonGeneric()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("CSVDestSimpleNonGeneric");
            s2C.InsertTestDataSet3();
            DbSource<string[]> source = new DbSource<string[]>(
                SqlConnection,
                "CSVDestSimpleNonGeneric"
            );

            //Act
            CsvDestination<string[]> dest = new CsvDestination<string[]>("./SimpleNonGeneric.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //Assert
            Assert.Equal(
                File.ReadAllText("./SimpleNonGeneric.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsSet3NoHeader.csv")
            );
        }
    }
}
