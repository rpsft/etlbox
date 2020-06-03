using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using CsvHelper.Configuration;
using Xunit;
using ETLBox.SqlServer;
using ETLBox.Csv;
using ETLBoxTests.Helper;

namespace ETLBoxTests.DataFlowTests
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
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/CsvSource/TwoColumns.csv");
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
        public void UsingClassMapAndNoHeader()
        {
            //Arrange
            FourColumnsTableFixture d4c = new FourColumnsTableFixture("CsvDestination4ColumnsClassMap");
            DbDestination<MyExtendedRow> dest = new DbDestination<MyExtendedRow>(Connection, "CsvDestination4ColumnsClassMap");

            //Act
            CsvSource<MyExtendedRow> source = new CsvSource<MyExtendedRow>("res/CsvSource/FourColumnsInvalidHeader.csv");
            source.Configuration.RegisterClassMap<ExtendedClassMap>();
            source.Configuration.HasHeaderRecord = false;
            source.Configuration.ShouldSkipRecord = ShouldSkipRecordDelegate;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d4c.AssertTestData();
        }

        private bool ShouldSkipRecordDelegate(string[] row)
        {
            if (row[0].Contains(".csv"))
                return true;
            else
                return false;
        }
    }
}
