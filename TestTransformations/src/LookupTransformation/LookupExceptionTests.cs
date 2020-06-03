using ETLBox;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBox.Exceptions;
using System;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class LookupExceptionTests
    {
        public LookupExceptionTests()
        {
        }

        public class MyDataRow
        {
            [MatchColumn("Key")]
            public long Col1 { get; set; }
            [RetrieveColumn("LookupValue")]
            public string Col2 { get; set; }
        }


        public class MyLookupRow
        {
            public long Key { get; set; }
            public long? LookupValue { get; set; }
        }


        [Fact]
        public void NoLookupSource()
        {
            //Arrange
            MemorySource<MyDataRow> source = new MemorySource<MyDataRow>();
            source.DataAsList.Add(new MyDataRow() { Col1 = 1, Col2 = "Test1" });

            //Act
            var lookup = new LookupTransformation<MyDataRow, MyLookupRow>();
            MemoryDestination<MyDataRow> dest = new MemoryDestination<MyDataRow>();

            //Assert
            Assert.Throws<ETLBoxException>(() =>
           {
               try
               {
                   source.LinkTo(lookup);
                   lookup.LinkTo(dest);
                   source.Execute();
                   dest.Wait();
               }
               catch (AggregateException e) { throw e.InnerException; }
           });
            //Assert
        }
    }
}
