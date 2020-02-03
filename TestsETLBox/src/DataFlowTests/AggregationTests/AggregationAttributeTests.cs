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
    public class AggregationAttributeTests
    {
        public AggregationAttributeTests()
        {
        }

        public class MySumRow
        {
            public int Id { get; set; }
            [AggregateColumn("AggValue", AggregationMethod.Sum)]
            public double DetailValue { get; set; }
        }

        public class MySumRowNullable
        {
            public int Id { get; set; }
            [AggregateColumn("AggValue", AggregationMethod.Sum)]
            public double? DetailValue { get; set; }
        }


        public class MyMaxRow
        {
            [AggregateColumn("AggValue", AggregationMethod.Max)]
            public float DetailValue { get; set; }
        }

        public class MyMinRow
        {
            [AggregateColumn("AggValue", AggregationMethod.Min)]
            public long? DetailValue { get; set; }
        }

        public class MyCountRow
        {
            [AggregateColumn("AggValue", AggregationMethod.Count)]
            public uint DetailValue { get; set; }
        }


        public class MyAggRow
        {
            public double? AggValue { get; set; }
        }

        [Fact]
        public void AggregateSum()
        {
            //Arrange
            List<MySumRow> sourceData = new List<MySumRow>()
                {
                new MySumRow { Id = 1,  DetailValue = 3.5 },
                new MySumRow { Id = 2,  DetailValue = 4.5 },
                new MySumRow { Id = 3,  DetailValue = 2.0 },
                };
            MemoryDestination<MyAggRow> dest = CreateFlow(sourceData);

            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 10)
                );
        }

        [Fact]
        public void AggregateSumWithNullable()
        {
            //Arrange
            List<MySumRowNullable> sourceData = new List<MySumRowNullable>()
                {
                new MySumRowNullable { Id = 1,  DetailValue = 3.5 },
                new MySumRowNullable { Id = 2,  DetailValue = 4.5 },
                new MySumRowNullable { Id = 3,  DetailValue = 2.0 },
                new MySumRowNullable { Id = 4,  DetailValue = null },
                };
            MemoryDestination<MyAggRow> dest = CreateFlow(sourceData);

            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 10)
                );
        }

        [Fact]
        public void AggregateMax()
        {
            //Arrange
            List<MyMaxRow> sourceData = new List<MyMaxRow>()
                {
                new MyMaxRow { DetailValue = 3.5F },
                new MyMaxRow { DetailValue = 4.5F },
                new MyMaxRow { DetailValue = 2.0F },
                };
            MemoryDestination<MyAggRow> dest = CreateFlow(sourceData);

            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 4.5F)
                );
        }

        [Fact]
        public void AggregateMin()
        {
            //Arrange
            List<MyMinRow> sourceData = new List<MyMinRow>()
                {
                new MyMinRow { DetailValue = 3 },
                new MyMinRow { DetailValue = 4 },
                new MyMinRow { DetailValue = 2 },
                };
            MemoryDestination<MyAggRow> dest = CreateFlow(sourceData);

            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 2)
                );
        }

        [Fact]
        public void AggregateCount()
        {
            //Arrange
            List<MyCountRow> sourceData = new List<MyCountRow>()
                {
                new MyCountRow { DetailValue = 5 },
                new MyCountRow { DetailValue = 7 },
                new MyCountRow { DetailValue = 8 },
                };
            MemoryDestination<MyAggRow> dest = CreateFlow(sourceData);

            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 3)
                );
        }


        private MemoryDestination<MyAggRow> CreateFlow<T>(List<T> sourceData)
        {
            MemorySource<T> source = new MemorySource<T>();
            source.Data = sourceData;

            Aggregation<T, MyAggRow> agg = new Aggregation<T, MyAggRow>();

            MemoryDestination<MyAggRow> dest = new MemoryDestination<MyAggRow>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();
            return dest;
        }

    }
}
