using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CSVSourceErrorLinkingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public CSVSourceErrorLinkingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }


        [Fact]
        public void WithObjectErrorLinking()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVSourceErrorLinking");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(SqlConnection, "CSVSourceErrorLinking");
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/CSVSource/TwoColumnsErrorLinking.csv");

            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }

        [Fact]
        public void WithoutErrorLinking()
        {
            //Arrange
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/CSVSource/TwoColumnsErrorLinking.csv");

            //Assert
            Assert.Throws<CsvHelper.TypeConversion.TypeConverterException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }
    }
}
