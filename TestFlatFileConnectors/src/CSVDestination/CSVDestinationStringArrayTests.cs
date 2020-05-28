using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvDestinationStringArrayTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvDestinationStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleNonGeneric()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("CSVDestSimpleNonGeneric");
            s2C.InsertTestDataSet3();
            DbSource<string[]> source = new DbSource<string[]>(SqlConnection, "CSVDestSimpleNonGeneric");

            //Act
            CsvDestination<string[]> dest = new CsvDestination<string[]>("./SimpleNonGeneric.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //Assert
            Assert.Equal(File.ReadAllText("./SimpleNonGeneric.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsSet3NoHeader.csv"));
        }


    }
}
