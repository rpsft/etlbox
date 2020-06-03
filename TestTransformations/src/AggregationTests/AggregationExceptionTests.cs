using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class AggregationExceptionTests
    {
        public AggregationExceptionTests()
        {
        }

        public class MyRow
        {
            public int Id { get; set; }
            public string ClassName { get; set; }
            public double DetailValue { get; set; }
        }

        public class MyAggRow
        {
            public string GroupName { get; set; }
            public double AggValue { get; set; }
        }

        [Fact]
        public void ExceptionInAggregationFunction()
        {
            //Arrange
            MemorySource<MyRow> source = new MemorySource<MyRow>();
            source.DataAsList = new List<MyRow>()
                {
                new MyRow { Id = 1,  DetailValue = 3.5 },
                };

            //Act
            Aggregation<MyRow, MyAggRow> agg = new Aggregation<MyRow, MyAggRow>(
                (row, aggRow) => throw new Exception("Test")
                );

            MemoryDestination<MyAggRow> dest = new MemoryDestination<MyAggRow>();

            //Assert
            source.LinkTo(agg);
            agg.LinkTo(dest);

            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void ExceptionInStoreKeyFunction()
        {
            //Arrange
            MemorySource<MyRow> source = new MemorySource<MyRow>();
            source.DataAsList = new List<MyRow>()
                {
                new MyRow { Id = 1, ClassName = "Class1", DetailValue = 3.5 }
                };

            //Act
            Aggregation<MyRow, MyAggRow> agg = new Aggregation<MyRow, MyAggRow>(
                (row, aggValue) => aggValue.AggValue += row.DetailValue,
                row => row.ClassName,
                (key, agg) => throw new Exception("Test")
                );

            MemoryDestination<MyAggRow> dest = new MemoryDestination<MyAggRow>();

            source.LinkTo(agg);
            agg.LinkTo(dest);

            //Assert
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });


        }

    }
}
