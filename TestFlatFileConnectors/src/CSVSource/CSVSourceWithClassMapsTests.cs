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
using CsvHelper;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceWithClassMapsTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvSourceWithClassMapsTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public string Col2 { get; set; }
            public int Col1 { get; set; }
        }

        public class ModelClassMap : ClassMap<MySimpleRow>
        {
            public ModelClassMap()
            {
                Map(m => m.Col1).Name("Header1");
                Map(m => m.Col2).Name("Header2");
            }
        }

        [Fact]
        public void SimpleFlowUsingClassMap()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvDestination2ColumnsClassMap");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "CsvDestination2ColumnsClassMap");

            //Act
            var source = new CsvSource<MySimpleRow>("res/CsvSource/TwoColumns.csv")
            {
                ClassMapType = typeof(ModelClassMap)
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        public class MyExtendedRow
        {
            public string Col2 { get; set; }
            public long? Col3 { get; set; }
            public decimal Col4 { get; set; }
        }

        public class ExtendedClassMap : ClassMap<MyExtendedRow>
        {
            public ExtendedClassMap()
            {
                Map(m => m.Col2).Index(0);
                Map(m => m.Col3).Index(1);
                Map(m => m.Col4).Index(2);
            }
        }


        [Fact]
        public void UsingClassMapAndNoHeader()
        {
            //Arrange
            FourColumnsTableFixture d4c = new FourColumnsTableFixture("CsvDestination4ColumnsClassMap");
            DbDestination<MyExtendedRow> dest = new DbDestination<MyExtendedRow>(Connection, "CsvDestination4ColumnsClassMap");

            //Act
            CsvSource<MyExtendedRow> source = new CsvSource<MyExtendedRow>("res/CsvSource/FourColumnsInvalidHeader.csv")
            {
                ClassMapType = typeof(ExtendedClassMap),
                Configuration =
                {
                    HasHeaderRecord = false,
                    ShouldSkipRecord = ShouldSkipRecordDelegate
                }
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d4c.AssertTestData();
        }

        private bool ShouldSkipRecordDelegate(ShouldSkipRecordArgs args)
        {
            return args.Record[0].Contains(".csv");
        }
    }
}
