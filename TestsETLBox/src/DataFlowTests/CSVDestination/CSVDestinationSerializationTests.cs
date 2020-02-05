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
                File.ReadAllText("res/CSVDestination/DateTimeSerialization.csv"));
        }


    }
}
