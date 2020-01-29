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
    public class LookupAttributeTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public LookupAttributeTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class LookupData
        {
            [ColumnMap("Col1")]
            public int Id { get; set; }
            [ColumnMap("Col2")]
            public string Value { get; set; }
        }

        public class InputDataRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
            //[MatchColumn("Id")]
            public int LookupId => Col1;
            //[RetrieveColumn("Value")]
            public string LookupValue { get; set; }
        }


        [Theory, MemberData(nameof(Connections))]
        public void InputTypeSameAsOutput(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "SourceLookupAttribute");
            source4Columns.InsertTestData();
            TwoColumnsTableFixture lookup2Columns = new TwoColumnsTableFixture(connection, "LookupAttribute");
            lookup2Columns.InsertTestData();

            DBSource<InputDataRow> source = new DBSource<InputDataRow>(connection, "SourceLookupAttribute");
            DBSource<LookupData> lookupSource = new DBSource<LookupData>(connection, "LookupAttribute");

            var lookup = new LookupTransformation<InputDataRow, LookupData>();
            lookup.Source = lookupSource;
            MemoryDestination<InputDataRow> dest = new MemoryDestination<InputDataRow>();
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
        }
    }
}
