using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class LookupStringArrayTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public LookupStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleLookupWithoutObject(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceNonGenericLookup");
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "DestinationNonGenericLookup", -1);
            FourColumnsTableFixture lookup4Columns = new FourColumnsTableFixture(connection, "LookupNonGeneric");
            lookup4Columns.InsertTestData();

            DbSource<string[]> source = new DbSource<string[]>(connection, "SourceNonGenericLookup");
            DbDestination<string[]> dest = new DbDestination<string[]>(connection, "DestinationNonGenericLookup");

            //Act
            List<string[]> lookupList = new List<string[]>();

            DbSource<string[]> lookupSource = new DbSource<string[]>(connection, "LookupNonGeneric");
            LookupTransformation<string[], string[]> lookup = new LookupTransformation<string[], string[]>(
                lookupSource,
                row =>
                {
                    Array.Resize(ref row, 4);
                    row[2] = lookupList.Where(lkupRow => lkupRow[0] == row[0]).Select(lkupRow => lkupRow[2]).FirstOrDefault();
                    row[3] = lookupList.Where(lkupRow => lkupRow[0] == row[0]).Select(lkupRow => lkupRow[3]).FirstOrDefault();
                    return row;
                },
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
