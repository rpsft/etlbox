using ALE.ETLBox.DataFlow;
using ClickHouse.Ado;

namespace TestTransformations.AggregationTests
{
    public class AggregationDynamicObjectTests
    {
        [Fact]
        public void GroupingUsingDynamicObject()
        {
            using var con = new ClickHouseConnection("Host=localhost;Port=9000;Database=default;User=clickhouse;Password=Qwe123456;");
            con.Open();
            using var cmd = con.CreateCommand("select 1");
            var res = cmd.ExecuteScalar();


            //Arrange
            var source = new MemorySource<ExpandoObject>();
            dynamic row1 = new ExpandoObject();
            row1.ClassName = "Class1";
            row1.DetailValue = 3.5;
            dynamic row2 = new ExpandoObject();
            row2.ClassName = "Class1";
            row2.DetailValue = 6.5;
            dynamic row3 = new ExpandoObject();
            row3.ClassName = "Class2";
            row3.DetailValue = 10;
            source.DataAsList.Add(row1);
            source.DataAsList.Add(row2);
            source.DataAsList.Add(row3);

            var agg = new Aggregation<
                ExpandoObject,
                ExpandoObject
            >(
                (row, aggValue) =>
                {
                    dynamic r = row;
                    dynamic a = aggValue;
                    if (!((IDictionary<string, object>)a).ContainsKey("AggValue"))
                        a.AggValue = r.DetailValue;
                    else
                        a.AggValue += r.DetailValue;
                },
                row =>
                {
                    dynamic r = row;
                    return r.ClassName;
                },
                (key, agg) =>
                {
                    dynamic a = agg;
                    a.GroupName = (string)key;
                }
            );

            var dest = new MemoryDestination<ExpandoObject>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                ar =>
                {
                    Assert.True(
                        ((dynamic)ar).AggValue == 10 && ((dynamic)ar).GroupName == "Class1"
                    );
                },
                ar =>
                {
                    Assert.True(
                        ((dynamic)ar).AggValue == 10 && ((dynamic)ar).GroupName == "Class2"
                    );
                }
            );
        }

        [Fact]
        public void AggregationIntUsingMappings()
        { 
            // Arrange
            var source = new MemorySource<ExpandoObject>();
            dynamic row1 = new ExpandoObject();
            row1.Id = 1;
            row1.Name = "a1";
            dynamic row2 = new ExpandoObject();
            row2.Id = 2;
            row2.Name = "a2";
            dynamic row3 = new ExpandoObject();
            row3.Id = 3;
            row3.Name = "a3";
            source.DataAsList.Add(row1);
            source.DataAsList.Add(row2);
            source.DataAsList.Add(row3);

            var agg = new Aggregation()
            { 
                Mappings = new Dictionary<string, InputAggregationField>
                { 
                    { "MaxId", new InputAggregationField{ Name = "Id", AggregationMethod = AggregationMethod.Max } },
                    { "MinId", new InputAggregationField{ Name = "Id", AggregationMethod = AggregationMethod.Min } },
                    { "Sum", new InputAggregationField{ Name = "Id", AggregationMethod = AggregationMethod.Sum } },
                    { "Count", new InputAggregationField{ Name = "Id", AggregationMethod = AggregationMethod.Count } }
                }
            };

            var dest = new MemoryDestination<ExpandoObject>();

            // Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            // Assert
            Assert.Single(dest.Data);
            var aggObj = dest.Data.First() as IDictionary<string, object>;
            Assert.Equal(3, aggObj["MaxId"]);
            Assert.Equal(1, aggObj["MinId"]);
            Assert.Equal(1 + 2 + 3, aggObj["Sum"]);
            Assert.Equal(3, aggObj["Count"]);
        }

        [Fact]
        public void AggregationGuidUsingMappings()
        { 
            // Arrange
            var source = new MemorySource<ExpandoObject>();
            dynamic row1 = new ExpandoObject();
            row1.Id = Guid.NewGuid();
            row1.Name = "a1";
            dynamic row2 = new ExpandoObject();
            row2.Id = Guid.NewGuid();
            row2.Name = "a2";
            dynamic row3 = new ExpandoObject();
            row3.Id = Guid.NewGuid();
            row3.Name = "a3";
            source.DataAsList.Add(row1);
            source.DataAsList.Add(row2);
            source.DataAsList.Add(row3);

            var agg = new Aggregation()
            { 
                Mappings = new Dictionary<string, InputAggregationField>
                { 
                    { "MaxId", new InputAggregationField{ Name = "Id", AggregationMethod = AggregationMethod.Max } },
                    { "MinId", new InputAggregationField{ Name = "Id", AggregationMethod = AggregationMethod.Min } },
                }
            };

            var dest = new MemoryDestination<ExpandoObject>();

            // Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            // Assert
            Assert.Single(dest.Data);

            var ids = source.Data
                .Select(d => d as IDictionary<string, object>)
                .Select(d => d["Id"])
                .ToArray();
            var aggObj = dest.Data.First() as IDictionary<string, object>;
            Assert.Equal(ids.Max(), aggObj["MaxId"]);
            Assert.Equal(ids.Min(), aggObj["MinId"]);
        }
    }
}
