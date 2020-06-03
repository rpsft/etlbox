using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
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
            source.DataAsList = new List<MyRow>()
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

        public class MyRowNullable
        {
            public int Id { get; set; }
            public double? DetailValue { get; set; }
        }

        public class MyAggRowNullable
        {
            public double? AggValue { get; set; } = 0;
        }

        [Fact]
        public void AggregateWithNull()
        {
            //Arrange
            MemorySource<MyRowNullable> source = new MemorySource<MyRowNullable>();
            source.DataAsList = new List<MyRowNullable>()
                {
                new MyRowNullable { Id = 1,  DetailValue = 3.5 },
                new MyRowNullable { Id = 0,  DetailValue = null },
                new MyRowNullable { Id = 2,  DetailValue = 4.5 },
                new MyRowNullable { Id = 3,  DetailValue = 2.0 },
                new MyRowNullable { Id = 4,  DetailValue = null },
                };

            Aggregation<MyRowNullable, MyAggRowNullable> agg = new Aggregation<MyRowNullable, MyAggRowNullable>(
                (row, aggRow) => aggRow.AggValue += row.DetailValue ?? 0
                );

            MemoryDestination<MyAggRowNullable> dest = new MemoryDestination<MyAggRowNullable>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<MyAggRowNullable>(dest.Data,
                ar => Assert.True(ar.AggValue == 10)
            );
        }
    }
}