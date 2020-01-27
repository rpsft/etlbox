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
    public class AggregationTests
    {
        public AggregationTests()
        {
        }

        public class MyRow
        {
            public int Id { get; set; }
            public double DetailValue { get; set; }
        }

        public class MyAggRow
        {
            public double AggValue { get; set; }
        }



        [Fact]
        public void AggregateSimple()
        {
            //Arrange
            MemorySource<MyRow> source = new MemorySource<MyRow>();
            source.Data = new List<MyRow>()
                {
                new MyRow { Id = 1,  DetailValue = 3.5 },
                new MyRow { Id = 2,  DetailValue = 4.5 },
                new MyRow { Id = 3,  DetailValue = 2.0 },
                };

            Aggregation<MyRow, MyAggRow> agg = new Aggregation<MyRow, MyAggRow>(
                (row, aggRow) => aggRow.AggValue += row.DetailValue
                );

            MemoryDestination<MyAggRow> dest = new MemoryDestination<MyAggRow>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 10)
                );
        }

    }
}
