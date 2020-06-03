using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using CsvHelper.Configuration.Attributes;
using System;
using System.IO;
using Xunit;
using ETLBox.SqlServer;
using ETLBox.Csv;
using ETLBoxTests.Helper;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvDestinationSerializationTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvDestinationSerializationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySeriRow
        {
            [Name("Header2")]
            [Index(2)]
            public DateTime Col2 { get; set; }
            [Name("Header1")]
            [Index(1)]
            public int Col1 { get; set; }
        }

        [Fact]
        public void SerializingDateTime()
        {
            int rowCount = 0;
            //Arrange
            CustomSource<MySeriRow> source = new CustomSource<MySeriRow>(
                () =>
                new MySeriRow()
                {
                    Col1 = 1,
                    Col2 = new DateTime(2010, 02, 05)
                },
                () => rowCount++ == 1);


            //Act
            CsvDestination<MySeriRow> dest = new CsvDestination<MySeriRow>("./DateTimeSerialization.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./DateTimeSerialization.csv"),
                File.ReadAllText("res/CsvDestination/DateTimeSerialization.csv"));
        }


    }
}
