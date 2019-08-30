using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow Source and Destination")]
    public class DBDestinationDifferentDBTests : IDisposable
    {
        public SqlConnectionManager ConnectionSource => Config.SqlConnectionManager("DataFlowSource");
        public SqlConnectionManager ConnectionDestination => Config.SqlConnectionManager("DataFlowDestination");
        public DBDestinationDifferentDBTests(DatabaseSourceDestinationFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        [Fact]
        public void TestTransferBetweenDBs()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(ConnectionSource, "Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(ConnectionDestination, "Destination");

            //Act
            DBSource source = new DBSource(ConnectionSource, "Source");
            DBDestination dest = new DBDestination(ConnectionDestination, "Destination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();

        }
    }
}
