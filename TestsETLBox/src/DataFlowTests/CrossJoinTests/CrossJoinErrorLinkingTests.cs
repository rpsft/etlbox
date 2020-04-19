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
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CrossJoinErrorLinkingTests
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
            MemorySource<string> source1 = new MemorySource<string>();
            source1.DataAsList = new List<string>() { "A", "B" };
            MemorySource<int> source2 = new MemorySource<int>();
            source2.DataAsList = new List<int>() { 1, 2, 3 };
            CrossJoin<string, int, string> crossJoin = new CrossJoin<string, int, string>(
                (data1, data2) =>
                {
                    if (data1 == "A") throw new Exception("Invalid record");
                    return data1 + data2.ToString();
                }
            );
            MemoryDestination<string> dest = new MemoryDestination<string>();
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source1.LinkTo(crossJoin.InMemoryTarget);
            source2.LinkTo(crossJoin.PassingTarget);
            crossJoin.LinkTo(dest);
            crossJoin.LinkErrorTo(errorDest);
            source1.Execute();
            source2.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Collection<ETLBoxError>(errorDest.Data,
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)),
                d => Assert.True(!string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText))
            );
        }
    }
}
