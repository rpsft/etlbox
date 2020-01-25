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
    public class LookupTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public LookupTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyLookupRow
        {
            [ColumnMap("Col1")]
            public long Key { get; set; }
            [ColumnMap("Col3")]
            public long? LookupValue1 { get; set; }
            [ColumnMap("Col4")]
            public decimal LookupValue2 { get; set; }
        }

        public class MyInputDataRow
        {
            public long Col1 { get; set; }
            public string Col2 { get; set; }
        }

        public class MyOutputDataRow
        {
            public long Col1 { get; set; }
            public string Col2 { get; set; }
            public long? Col3 { get; set; }
            public decimal Col4 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleLookupFromDB(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "SourceSimple");
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "DestinationSimple");
            FourColumnsTableFixture lookup4Columns = new FourColumnsTableFixture(connection, "LookupSimple");
            lookup4Columns.InsertTestData();

            DBSource<MyInputDataRow> source = new DBSource<MyInputDataRow>(connection, "SourceSimple");
            DBSource<MyLookupRow> lookupSource = new DBSource<MyLookupRow>(connection, "LookupSimple");

            //Act
            List<MyLookupRow> LookupTableData = new List<MyLookupRow>();
            Lookup<MyInputDataRow, MyOutputDataRow, MyLookupRow> lookup = new Lookup<MyInputDataRow, MyOutputDataRow, MyLookupRow>(
                row =>
                {
                    MyOutputDataRow output = new MyOutputDataRow()
                    {
                        Col1 = row.Col1,
                        Col2 = row.Col2,
                        Col3 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue1).FirstOrDefault(),
                        Col4 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue2).FirstOrDefault(),
                    };
                    return output;
                }
                , lookupSource
                , LookupTableData
            );
            DBDestination<MyOutputDataRow> dest = new DBDestination<MyOutputDataRow>(connection, "DestinationSimple");
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        public void InputTypeSameAsOutput(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "SourceLookupSameType");
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "DestinationLookupSameType");
            FourColumnsTableFixture lookup4Columns = new FourColumnsTableFixture(connection, "LookupSameType");
            lookup4Columns.InsertTestData();

            DBSource<MyOutputDataRow> source = new DBSource<MyOutputDataRow>(connection, "SourceLookupSameType");
            DBSource<MyLookupRow> lookupSource = new DBSource<MyLookupRow>(connection, "LookupSameType");

            var lookup = new ETLBox.DataFlow.Lookup<MyOutputDataRow, MyLookupRow>();
            lookup.RowTransformationFunc =
                row =>
                {

                    row.Col1 = row.Col1;
                    row.Col2 = row.Col2;
                    row.Col3 = lookup.LookupList.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue1).FirstOrDefault();
                    row.Col4 = lookup.LookupList.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue2).FirstOrDefault();
                    return row;
                };
            lookup.Source = lookupSource;
            DBDestination<MyOutputDataRow> dest = new DBDestination<MyOutputDataRow>(connection, "DestinationLookupSameType");
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
