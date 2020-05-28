using ALE.ETLBox.DataFlow;
using System.Collections.Generic;
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
            [AggregateColumn("DetailValue", AggregationMethod.Sum)]
            public double AggValue { get; set; }
        }

        public class MySumRowNullable
        {
            [AggregateColumn("DetailValue", AggregationMethod.Sum)]
            public double? AggValue { get; set; }
        }


        public class MyMaxRow
        {
            [AggregateColumn("DetailValue", AggregationMethod.Max)]
            public float AggValue { get; set; }
        }

        public class MyMinRow
        {
            [AggregateColumn("DetailValue", AggregationMethod.Min)]
            public long? AggValue { get; set; }
        }

        public class MyCountRow
        {
            [AggregateColumn("DetailValue", AggregationMethod.Count)]
            public uint AggValue { get; set; }
        }


        public class MyInputRow
        {
            public int Id { get; set; }
            public double? DetailValue { get; set; }
        }

        [Fact]
        public void AggregateSum()
        {
            //Arrange
            List<MyInputRow> sourceData = new List<MyInputRow>()
                {
                new MyInputRow { Id = 1,  DetailValue = 3.5 },
                new MyInputRow { Id = 2,  DetailValue = 4.5 },
                new MyInputRow { Id = 3,  DetailValue = 2.0 },
                };
            MemoryDestination<MySumRow> dest = CreateFlow<MySumRow>(sourceData);

            //Assert
            Assert.Collection(dest.Data,
                ar => Assert.True(ar.AggValue == 10)
                );
        }

        [Fact]
        public void AggregateSumWithNullable()
        {
            //Arrange
            List<MyInputRow> sourceData = new List<MyInputRow>()
                {
                new MyInputRow { Id = 1,  DetailValue = 3.5 },
                new MyInputRow { Id = 2,  DetailValue = 4.5 },
                new MyInputRow { Id = 3,  DetailValue = 2.0 },
                new MyInputRow { Id = 4,  DetailValue = null },
                };
            MemoryDestination<MySumRowNullable> dest = CreateFlow<MySumRowNullable>(sourceData);

            //Assert
            Assert.Collection(dest.Data,
                ar => Assert.True(ar.AggValue == 10)
                );
        }

        [Fact]
        public void AggregateMax()
        {
            //Arrange
            List<MyInputRow> sourceData = new List<MyInputRow>()
                {
                new MyInputRow { DetailValue = 3.5F },
                new MyInputRow { DetailValue = 4.5F },
                new MyInputRow { DetailValue = 2.0F },
                };
            MemoryDestination<MyMaxRow> dest = CreateFlow<MyMaxRow>(sourceData);

            //Assert
            Assert.Collection(dest.Data,
                ar => Assert.True(ar.AggValue == 4.5F)
                );
        }

        [Fact]
        public void AggregateMin()
        {
            //Arrange
            List<MyInputRow> sourceData = new List<MyInputRow>()
                {
                new MyInputRow { DetailValue = 3 },
                new MyInputRow { DetailValue = 4 },
                new MyInputRow { DetailValue = 2 },
                };
            MemoryDestination<MyMinRow> dest = CreateFlow<MyMinRow>(sourceData);

            //Assert
            Assert.Collection(dest.Data,
                ar => Assert.True(ar.AggValue == 2)
                );
        }

        [Fact]
        public void AggregateCount()
        {
            //Arrange
            List<MyInputRow> sourceData = new List<MyInputRow>()
                {
                new MyInputRow { DetailValue = 5 },
                new MyInputRow { DetailValue = 7 },
                new MyInputRow { DetailValue = 8 },
                };
            MemoryDestination<MyCountRow> dest = CreateFlow<MyCountRow>(sourceData);

            //Assert
            Assert.Collection(dest.Data,
                ar => Assert.True(ar.AggValue == 3)
                );
        }


        private MemoryDestination<T> CreateFlow<T>(List<MyInputRow> sourceData)
        {
            MemorySource<MyInputRow> source = new MemorySource<MyInputRow>();
            source.DataAsList = sourceData;

            Aggregation<MyInputRow, T> agg = new Aggregation<MyInputRow, T>();

            MemoryDestination<T> dest = new MemoryDestination<T>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();
            return dest;
        }

    }
}
