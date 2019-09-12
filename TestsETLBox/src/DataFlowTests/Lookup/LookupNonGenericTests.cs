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
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class LookupNonGenericTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public LookupNonGenericTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleLookupWithoutObject(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection,"Source");
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection,"Destination", -1);
            FourColumnsTableFixture lookup4Columns = new FourColumnsTableFixture(connection,"Lookup");
            lookup4Columns.InsertTestData();

            DBSource source = new DBSource(connection, "Source");
            DBDestination dest = new DBDestination(connection, "Destination");

            //Act
            List<string[]> lookupList = new List<string[]>();

            DBSource lookupSource = new DBSource(connection, "Lookup");
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
