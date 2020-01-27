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
    public class AggregationUsingMatchingTests
    {
        public AggregationUsingMatchingTests()
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
            public string ClassName { get; set; }
            public double AggValue { get; set; }
        }

        [Fact]
        public void ClassificationAndKeepingKey()
        {
            //Arrange
            MemorySource<MyRow> source = new MemorySource<MyRow>();
            source.Data = new List<MyRow>()
                {
                new MyRow { Id = 1, ClassName = "Class1", DetailValue = 3.5 },
                new MyRow { Id = 2, ClassName = "Class1", DetailValue = 6.5 },
                new MyRow { Id = 3, ClassName = "Class2", DetailValue = 1.2 },
                new MyRow { Id = 4, ClassName = "Class2", DetailValue = 2.3 },
                new MyRow { Id = 5, ClassName = "Class2", DetailValue = 16.5 },
                new MyRow { Id = 6, ClassName = "Class3", DetailValue = 30.0 },
                };

            Aggregation<MyRow, MyAggRow> agg = new Aggregation<MyRow, MyAggRow>(
                (row, aggValue) => aggValue.AggValue += row.DetailValue,
                row => row.ClassName,
                (key, agg) => agg.ClassName = (string)key
                );

            MemoryDestination<MyAggRow> dest = new MemoryDestination<MyAggRow>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();


            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 10 && ar.ClassName == "Class1"),
                ar => Assert.True(ar.AggValue == 20 && ar.ClassName == "Class2"),
                ar => Assert.True(ar.AggValue == 30 && ar.ClassName == "Class3")
            );
        }

    }
}
