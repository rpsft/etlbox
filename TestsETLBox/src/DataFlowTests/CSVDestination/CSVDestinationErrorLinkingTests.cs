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
    public class CSVDestinationErrorLinkingTests
    {
        public CSVDestinationErrorLinkingTests()
        {

        }

        public class MySimpleRow
        {
            private static int ExceptionCountX = 0;
            private static int ExceptionCountNull = 0;
            public string Col1 { get; set; }
            public string Col2
            {
                get
                {
                    if (ExceptionCountX == 0 && Col1 == "X") {
                        ExceptionCountX++;
                        throw new Exception("Error record!");
                    }
                    if (ExceptionCountNull == 0 && Col1 == null)
                    {
                        ExceptionCountNull++;
                        throw new Exception("Error record!");
                    }
                    else
                        return "Test" + Col1;
                }
            }
        }

        [Fact]
        public void RedirectBatch()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.Data = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = "X" },
                new MySimpleRow() { Col1 = "1" },
                new MySimpleRow() { Col1 = "2" },
                new MySimpleRow() { Col1 = null },
                new MySimpleRow() { Col1 = "3" },

            };
            CSVDestination<MySimpleRow> dest = new CSVDestination<MySimpleRow>("ErrorFile.csv", batchSize: 1);
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            dest.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./ErrorFile.csv"),
                 File.ReadAllText("res/CSVDestination/TwoColumnsErrorLinking.csv"));
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                 d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }


        [Fact]
        public void NoErrorHandling()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.Data = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = "X" },
                new MySimpleRow() { Col1 = "1" },
                new MySimpleRow() { Col1 = null }
            };
            CSVDestination<MySimpleRow> dest = new CSVDestination<MySimpleRow>("ErrorFile.csv", batchSize: 2);

            //Act
            //Assert
            Assert.ThrowsAny<Exception>(() =>
           {
               source.LinkTo(dest);
               source.Execute();
               dest.Wait();
           });
        }
    }
}
