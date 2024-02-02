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
    }
}
