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

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class LookupTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public LookupTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MyLookupRow
        {
            [ColumnMap("Col1")]
            public int Key { get; set; }
            [ColumnMap("Col3")]
            public long? LookupValue1 { get; set; }
            [ColumnMap("Col4")]
            public decimal LookupValue2 { get; set; }
        }

        public class MyInputDataRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        public class MyOutputDataRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
            public long? Col3 { get; set; }
            public decimal Col4 { get; set; }
        }

        [Fact]
        public void SimpleLookupFromDB()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("Destination");
            FourColumnsTableFixture lookup4Columns = new FourColumnsTableFixture("Lookup");
            lookup4Columns.InsertTestData();

            DBSource<MyInputDataRow> source = new DBSource<MyInputDataRow>(Connection, "Source");
            DBSource<MyLookupRow> lookupSource = new DBSource<MyLookupRow>(Connection, "Lookup");

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
            DBDestination<MyOutputDataRow> dest = new DBDestination<MyOutputDataRow>(Connection, "Destination");
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
