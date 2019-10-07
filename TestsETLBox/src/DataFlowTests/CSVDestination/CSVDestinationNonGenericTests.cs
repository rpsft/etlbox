using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CSVDestinationNonGenericTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CSVDestinationNonGenericTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }


        [Fact]
        public void SimpleNonGeneric()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("CSVDestSimpleNonGeneric");
            s2C.InsertTestData();
            DBSource source = new DBSource(SqlConnection, "CSVDestSimpleNonGeneric");

            //Act
            CSVDestination dest = new CSVDestination("./SimpleNonGeneric.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //Assert
            Assert.Equal(File.ReadAllText("./SimpleNonGeneric.csv"),
                File.ReadAllText("res/CSVDestination/TwoColumnsNoHeader.csv"));
        }


    }
}
