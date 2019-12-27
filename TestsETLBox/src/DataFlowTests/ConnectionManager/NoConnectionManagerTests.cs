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
        public void DBSource()
        {
            //Arrange
            DBSource source = new DBSource("test");
            MemoryDestination dest = new MemoryDestination();
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void DBDestination()
        {
            //Arrange
            string[] data = { "1", "2" };
            MemorySource source = new MemorySource();
            source.Data.Add(data);
            DBDestination dest = new DBDestination("test");
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
