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
    public class CrossJoinNullHandlingTests
    {
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreNullValues()
        {
            //Arrange
            MemorySource<string> source1 = new MemorySource<string>();
            source1.Data = new List<string>() { "A", null, "B", "C"};
            MemorySource<int?> source2 = new MemorySource<int?>();
            source2.Data = new List<int?>() { 1, null, 2 , null, 3};
            CrossJoin<string, int?, string> crossJoin = new CrossJoin<string, int?, string>(
                (data1, data2) =>
                {
                    if (data1 == "C") return null;
                    else return data1 + data2?.ToString();
                }
            );
            MemoryDestination<string> dest = new MemoryDestination<string>();


            //Act
            source1.LinkTo(crossJoin.InMemoryTarget);
            source2.LinkTo(crossJoin.PassingTarget);
            crossJoin.LinkTo(dest);
            source1.Execute();
            source2.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, dest.Data.Count);
            Assert.Collection<string>(dest.Data,
                s => Assert.Equal("A1", s),
                s => Assert.Equal("B1", s),
                s => Assert.Equal("A2", s),
                s => Assert.Equal("B2", s),
                s => Assert.Equal("A3", s),
                s => Assert.Equal("B3", s)
                );
        }
    }
}
