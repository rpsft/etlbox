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
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class RowTransformationMultipleLinkTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public RowTransformationMultipleLinkTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void Linking3Transformations()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("SourceMultipleLinks");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DestinationMultipleLinks");

            DBSource source = new DBSource(SqlConnection, "SourceMultipleLinks");
            DBDestination dest = new DBDestination(SqlConnection, "DestinationMultipleLinks");
            RowTransformation trans1 = new RowTransformation(row => row);
            RowTransformation trans2 = new RowTransformation(row => row);
            RowTransformation trans3 = new RowTransformation(row => row);

            //Act
            source.LinkTo(trans1).LinkTo(trans2).LinkTo(trans3).LinkTo(dest);
            Task sourceT = source.ExecuteAsync();
            Task destT = dest.Completion();

            //Assert
            sourceT.Wait();
            destT.Wait();
            dest2Columns.AssertTestData();
        }


    }
}
