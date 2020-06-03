using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class AggregationDynamicObjectTests
    {
        public AggregationDynamicObjectTests()
        {
        }

        [Fact]
        public void GroupingUsingDynamicObject()
        {
            //Arrange
            MemorySource<ExpandoObject> source = new MemorySource<ExpandoObject>();
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

            Aggregation<ExpandoObject, ExpandoObject> agg = new Aggregation<ExpandoObject, ExpandoObject>(
                (row, aggValue) =>
                {
                    dynamic r = row as ExpandoObject;
                    dynamic a = aggValue as ExpandoObject;
                    if (!((IDictionary<String, object>)a).ContainsKey("AggValue"))
                        a.AggValue = r.DetailValue;
                    else
                        a.AggValue += r.DetailValue;
                },
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    return r.ClassName;
                },
                (key, agg) =>
                {
                    dynamic a = agg as ExpandoObject;
                    a.GroupName = (string)key;
                });

            MemoryDestination<ExpandoObject> dest = new MemoryDestination<ExpandoObject>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();


            //Assert
            Assert.Collection<ExpandoObject>(dest.Data,
                ar =>
                {
                    dynamic a = ar as ExpandoObject;
                    Assert.True(a.AggValue == 10 && a.GroupName == "Class1");
                },
                ar =>
                {
                    dynamic a = ar as ExpandoObject;
                    Assert.True(a.AggValue == 10 && a.GroupName == "Class2");
                }
            );
        }

    }
}
