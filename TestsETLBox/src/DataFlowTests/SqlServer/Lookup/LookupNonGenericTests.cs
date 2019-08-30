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
    [Collection("Sql Server DataFlow")]
    public class LookupNonGenericTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public LookupNonGenericTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        [Fact]
        public void SimpleLookupWithoutObject()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("Destination", -1);
            FourColumnsTableFixture lookup4Columns = new FourColumnsTableFixture("Lookup");
            lookup4Columns.InsertTestData();

            DBSource source = new DBSource(Connection, "Source");
            DBDestination dest = new DBDestination(Connection,"Destination");

            //Act
            List<string[]> lookupList = new List<string[]>();

            DBSource lookupSource = new DBSource(Connection, "Lookup");
            Lookup lookup = new Lookup(
                row =>
                {
                    Array.Resize(ref row, 4);
                    row[2] = lookupList.Where(lkupRow => lkupRow[0] == row[0]).Select(lkupRow => lkupRow[2]).FirstOrDefault();
                    row[3] = lookupList.Where(lkupRow => lkupRow[0] == row[0]).Select(lkupRow => lkupRow[3]).FirstOrDefault();
                    return row;
                },
                lookupSource,
                lookupList
            );

            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
