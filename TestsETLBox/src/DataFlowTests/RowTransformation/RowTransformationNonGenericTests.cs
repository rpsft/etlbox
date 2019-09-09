using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationNonGenericTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public RowTransformationNonGenericTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void RearrangeSwappedData()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationRowTransformation");
            CSVSource source = new CSVSource("res/RowTransformation/TwoColumnsSwapped.csv");

            //Act
            RowTransformation trans = new RowTransformation(
                csvdata =>
                {
                    return new string[] { csvdata[1], csvdata[0] };
                });

            DBDestination dest = new DBDestination(Connection, "DestinationRowTransformation");
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }


    }
}
