using ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CustomDestinationErrorLinkingTests
    {
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void TestErrorLink()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = new List<MySimpleRow>()
            {
                new MySimpleRow() { Col1 = 1, Col2 = "Test1"},
                new MySimpleRow() { Col1 = 2, Col2 = "ErrorRecord"},
                new MySimpleRow() { Col1 = 3, Col2 = "Test3"},
            };
            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
               row =>
               {
                   if (row.Col1 == 2)
                       throw new Exception("Error record!");
               }
           );
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            dest.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }
    }
}
