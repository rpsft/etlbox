using System.Diagnostics.CodeAnalysis;
using ALE.ETLBox.src.Toolbox.DataFlow;

namespace TestTransformations.src.AggregationTests
{
    [SuppressMessage("ReSharper", "CompareOfFloatsByEqualityOperator")]
    public sealed class AggregationStringArrayTests : IDisposable
    {
        private readonly CultureInfo _culture;

        public AggregationStringArrayTests()
        {
            _culture = CultureInfo.CurrentCulture;
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
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(new[] { "Class1", "3.5" });
            source.DataAsList.Add(new[] { "Class1", "6.5" });
            source.DataAsList.Add(new[] { "Class2", "10" });

            Aggregation<string[], MyAggRow> agg = new Aggregation<string[], MyAggRow>(
                (row, aggValue) => aggValue.AggValue += Convert.ToDouble(row[1]),
                row => row[0],
                (key, agg) => agg.GroupName = (string)key
            );

            var dest = new MemoryDestination<MyAggRow>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                ar => Assert.True(ar.AggValue == 10 && ar.GroupName == "Class1"),
                ar => Assert.True(ar.AggValue == 10 && ar.GroupName == "Class2")
            );
        }

        public void Dispose()
        {
            CultureInfo.CurrentCulture = _culture;
        }
    }
}
