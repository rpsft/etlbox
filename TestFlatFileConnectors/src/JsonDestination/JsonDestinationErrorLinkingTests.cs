using ETLBox.DataFlow;
using ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonDestinationErrorLinkingTests
    {
        public JsonDestinationErrorLinkingTests()
        {

        }

        public class MySimpleRow
        {
            public string Col1 { get; set; }
            public string Col2
            {
                get
                {
                    if (Col1 == null || Col1 == "X")
                        throw new Exception("Error record!");
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
            source.DataAsList = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = "X" },
                new MySimpleRow() { Col1 = "1" },
                new MySimpleRow() { Col1 = "2" },
                new MySimpleRow() { Col1 = null },
                new MySimpleRow() { Col1 = "3" },

            };
            JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>("ErrorFile.json", ResourceType.File);
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            dest.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./ErrorFile.json"),
                 File.ReadAllText("res/JsonDestination/TwoColumnsErrorLinking.json"));
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
            source.DataAsList = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = "X" },
                new MySimpleRow() { Col1 = "1" },
                new MySimpleRow() { Col1 = null }
            };
            JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>("ErrorFile.json", ResourceType.File);

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
