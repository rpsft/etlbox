using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace ALE.ETLBoxTests.Performance
{
    [Collection("Performance")]
    public class MemoryDestinationTests
    {
        private readonly ITestOutputHelper output;

        public MemoryDestinationTests(ITestOutputHelper output)
        {
            this.output = output;
        }


        /*
         * X Rows with 1027 bytes per Row (1020 bytes data + 7 bytes for sql server)
         */
        [Theory,
            InlineData(100000, 10000, 0.5)]
        public void CompareFlowWithBulkInsert(int numberOfRows, int batchSize, double deviation)
        {
            //Arrange
            BigDataCsvSource.CreateCSVFileIfNeeded(numberOfRows);

            var sourceNonGeneric = new CSVSource(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var destNonGeneric = new MemoryDestination(batchSize);
            var sourceGeneric = new CSVSource<CSVData>(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var destGeneric = new MemoryDestination<CSVData>(batchSize);
            var sourceDynamic = new CSVSource<ExpandoObject>(BigDataCsvSource.GetCompleteFilePath(numberOfRows));
            var destDynamic = new MemoryDestination<ExpandoObject>(batchSize);


            //Act
            var teNonGeneric = GetETLBoxTime(numberOfRows, sourceNonGeneric, destNonGeneric);
            var teGeneric = GetETLBoxTime(numberOfRows, sourceGeneric, destGeneric);
            var teDynamic = GetETLBoxTime(numberOfRows, sourceDynamic, destDynamic);

            //Assert
            Assert.Equal(numberOfRows, destNonGeneric.Data.Count);
            Assert.Equal(numberOfRows, destGeneric.Data.Count);
            Assert.Equal(numberOfRows, destDynamic.Data.Count);
            Assert.True(new [] { teGeneric.TotalMilliseconds, teNonGeneric.TotalMilliseconds, teDynamic.TotalMilliseconds }.Max() <
                 new [] { teGeneric.TotalMilliseconds, teNonGeneric.TotalMilliseconds, teDynamic.TotalMilliseconds }.Max() * (deviation+1));
        }

        private TimeSpan GetETLBoxTime<T>(int numberOfRows, CSVSource<T> source, MemoryDestination<T> dest)
        {
            source.LinkTo(dest);
            var timeElapsedETLBox = BigDataHelper.LogExecutionTime($"Copying Csv into DB (non generic) with {numberOfRows} rows of data using ETLBox",
                () =>
                {
                    source.Execute();
                    dest.Wait();
                }
            );
            if(typeof(T) == typeof(string[]))
                output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Non generic).");
            else
                output.WriteLine("Elapsed " + timeElapsedETLBox.TotalSeconds + " seconds for ETLBox (Generic).");
            return timeElapsedETLBox;
        }
    }
}
