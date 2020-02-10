using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class NoConnectionManagerTests
    {
        public NoConnectionManagerTests()
        {
        }

        [Fact]
        public void DbSource()
        {
            //Arrange
            DbSource<string[]> source = new DbSource<string[]>("test");
            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void DbDestination()
        {
            //Arrange
            string[] data = { "1", "2" };
            MemorySource<string[]> source = new MemorySource<string[]>();
            source.Data.Add(data);
            DbDestination<string[]> dest = new DbDestination<string[]>("test");
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                try
                {
                    source.Execute();
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            });
        }


    }
}
