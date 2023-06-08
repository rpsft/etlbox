using System.IO;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using CsvHelper.Configuration.Attributes;
using JetBrains.Annotations;
using TestFlatFileConnectors.Fixtures;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.CSVDestination
{
    [Collection("DataFlow")]
    public class CsvDestinationConfigurationTests : IClassFixture<DataFlowDatabaseFixture>
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        public class MySimpleRow
        {
            [Index(1)]
            public int Col1 { get; set; }

            [Index(2)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void DisableHeader()
        {
            //Arrange
            var s2c = new TwoColumnsTableFixture("CsvSourceNoHeader");
            s2c.InsertTestData();
            var source = new DbSource<MySimpleRow>(SqlConnection, "CsvSourceNoHeader");

            //Act
            var dest = new CsvDestination<MySimpleRow>("./ConfigurationNoHeader.csv")
            {
                Configuration = { HasHeaderRecord = false }
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("./ConfigurationNoHeader.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsNoHeader.csv")
            );
        }
    }
}
