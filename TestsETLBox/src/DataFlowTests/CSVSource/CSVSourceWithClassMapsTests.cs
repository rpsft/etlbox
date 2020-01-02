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
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CSVSourceWithClassMapsTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public CSVSourceWithClassMapsTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVDestination2ColumnsClassMap");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "CSVDestination2ColumnsClassMap");

            //Act
            CSVSource<MySimpleRow> source = new CSVSource<MySimpleRow>("res/CSVSource/TwoColumns.csv");
            source.Configuration.RegisterClassMap<ModelClassMap>();
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
        public void ExtendedFlowUsingClassMap()
        {
            //Arrange
            FourColumnsTableFixture d4c = new FourColumnsTableFixture("CSVDestination4ColumnsClassMap");
            DBDestination<MyExtendedRow> dest = new DBDestination<MyExtendedRow>(Connection, "CSVDestination4ColumnsClassMap");

            //Act
            CSVSource<MyExtendedRow> source = new CSVSource<MyExtendedRow>("res/CSVSource/FourColumns.csv");
            source.Configuration.RegisterClassMap<ExtendedClassMap>();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d4c.AssertTestData();
        }
    }
}
