using ETLBox.DataFlow; using ETLBox.DataFlow.Connectors; using ETLBox.DataFlow.Transformations;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class AggregationUsingGroupingTests
    {
        public AggregationUsingGroupingTests()
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
        public void GroupingAndKeepingKey()
        {
            //Arrange
            MemorySource<MyRow> source = new MemorySource<MyRow>();
            source.DataAsList = new List<MyRow>()
                {
                new MyRow { Id = 1, ClassName = "Class1", DetailValue = 3.5 },
                new MyRow { Id = 2, ClassName = "Class1", DetailValue = 6.5 },
                new MyRow { Id = 3, ClassName = "Class2", DetailValue = 1.2 },
                new MyRow { Id = 4, ClassName = "Class2", DetailValue = 2.3 },
                new MyRow { Id = 5, ClassName = "Class2", DetailValue = 16.5 },
                new MyRow { Id = 6, ClassName = "Class3", DetailValue = 30.0 },
                new MyRow { Id = 6, ClassName = null, DetailValue = 14.5 },
                new MyRow { Id = 6, ClassName = null, DetailValue = 15.5 },
                };

            Aggregation<MyRow, MyAggRow> agg = new Aggregation<MyRow, MyAggRow>(
                (row, aggValue) => aggValue.AggValue += row.DetailValue,
                row => row.ClassName,
                (key, agg) => agg.GroupName = (string)key
                );

            MemoryDestination<MyAggRow> dest = new MemoryDestination<MyAggRow>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();


            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 10 && ar.GroupName == "Class1"),
                ar => Assert.True(ar.AggValue == 20 && ar.GroupName == "Class2"),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupName == "Class3"),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupName == string.Empty)
            );
        }

    }
}
