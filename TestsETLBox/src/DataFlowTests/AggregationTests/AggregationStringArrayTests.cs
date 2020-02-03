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
using System.Dynamic;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class AggregationStringArrayTests
    {
        public AggregationStringArrayTests()
        {
        }

        public class MyAggRow
        {
            public string GroupName { get; set; }
            public double AggValue { get; set; }
        }

        [Fact]
        public void GroupingUsingStringArray()
        {
            //Arrange
            MemorySource<string[]> source = new MemorySource<string[]>();
            source.Data.Add(new string[] { "Class1", "3.5" });
            source.Data.Add(new string[] { "Class1", "6.5" });
            source.Data.Add(new string[] { "Class2", "10" });

            Aggregation<string[], MyAggRow> agg = new Aggregation<string[], MyAggRow>(
                (row, aggValue) => aggValue.AggValue += Convert.ToDouble(row[1]),
                row => row[0],
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
              ar => Assert.True(ar.AggValue == 10 && ar.GroupName == "Class2")
          );
        }

    }
}
