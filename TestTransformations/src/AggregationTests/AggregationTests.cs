using ALE.ETLBox.DataFlow;

namespace TestTransformations.AggregationTests
{
    [Collection("DataFlow")]
    public class AggregationTests
    {
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
            MemorySource<MyRow> source = new MemorySource<MyRow>
            {
                DataAsList = new List<MyRow>
                {
                    new() { Id = 1, DetailValue = 3.5 },
                    new() { Id = 2, DetailValue = 4.5 },
                    new() { Id = 3, DetailValue = 2.0 },
                }
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
            Assert.Collection(dest.Data, ar => Assert.True(ar.AggValue == 10));
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
            MemorySource<MyRowNullable> source = new MemorySource<MyRowNullable>
            {
                DataAsList = new List<MyRowNullable>
                {
                    new() { Id = 1, DetailValue = 3.5 },
                    new() { Id = 0, DetailValue = null },
                    new() { Id = 2, DetailValue = 4.5 },
                    new() { Id = 3, DetailValue = 2.0 },
                    new() { Id = 4, DetailValue = null },
                }
            };

            Aggregation<MyRowNullable, MyAggRowNullable> agg = new Aggregation<
                MyRowNullable,
                MyAggRowNullable
            >((row, aggRow) => aggRow.AggValue += row.DetailValue ?? 0);

            MemoryDestination<MyAggRowNullable> dest = new MemoryDestination<MyAggRowNullable>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(dest.Data, ar => Assert.True(ar.AggValue == 10));
        }
    }
}
