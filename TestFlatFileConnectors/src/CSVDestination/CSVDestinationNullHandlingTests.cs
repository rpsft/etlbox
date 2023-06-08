using System.Collections.Generic;
using System.IO;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using CsvHelper.Configuration.Attributes;
using TestShared.Helper;
using Xunit;

namespace TestFlatFileConnectors.CSVDestination
{
    [Collection("DataFlow")]
    public class CsvDestinationNullHandlingTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            [Index(0)]
            public int Col1 { get; set; }

            [Index(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>
            {
                DataAsList = new List<MySimpleRow>
                {
                    null,
                    new MySimpleRow { Col1 = 1, Col2 = "Test1" },
                    null,
                    new MySimpleRow { Col1 = 2, Col2 = "Test2" },
                    new MySimpleRow { Col1 = 3, Col2 = "Test3" },
                    null
                }
            };

            //Act
            CsvDestination<MySimpleRow> dest = new CsvDestination<MySimpleRow>(
                "./IgnoreNullValues.csv"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("./IgnoreNullValues.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumns.csv")
            );
        }

        [Fact]
        public void IgnoreWithStringArray()
        {
            //Arrange
            MemorySource<string[]> source = new MemorySource<string[]>
            {
                DataAsList = new List<string[]>
                {
                    null,
                    new[] { "1", "Test1" },
                    null,
                    new[] { "2", "Test2" },
                    new[] { "3", "Test3" },
                    null
                }
            };

            //Act
            CsvDestination<string[]> dest = new CsvDestination<string[]>(
                "./IgnoreNullValuesStringArray.csv"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("./IgnoreNullValuesStringArray.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsNoHeader.csv")
            );
        }
    }
}
