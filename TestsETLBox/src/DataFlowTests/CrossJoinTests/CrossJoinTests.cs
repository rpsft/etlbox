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
    public class CrossJoinTests
    {
        public CrossJoinTests()
        {
        }

        [Fact]
        public void MergeJoinUsingOneObject()
        {
            //Arrange
            MemorySource<string> source1 = new MemorySource<string>();
            source1.Data = new List<string>() { "A", "B" };
            MemorySource<int> source2 = new MemorySource<int>();
            source2.Data = new List<int>() { 1, 2, 3 };
            CrossJoin<string, int, string> crossJoin = new CrossJoin<string, int, string>(
                (data1, data2) => data1 + data2.ToString()
                );
            MemoryDestination<string> dest = new MemoryDestination<string>();


            //Act
            source1.LinkTo(crossJoin.SmallTableDest);
            source2.LinkTo(crossJoin.BigTableDest);
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
