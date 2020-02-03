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
    public class AggregationGroupingAttributeTests
    {
        public AggregationGroupingAttributeTests()
        {
        }

        public class MyRow
        {
            public int Id { get; set; }
            [GroupColumn("GroupName")]
            public string ClassName { get; set; }
            [AggregateColumn("AggValue", AggregationMethod.Sum)]
            public double DetailValue { get; set; }
        }

        public class MyAggRow
        {
            public string GroupName { get; set; }
            public double AggValue { get; set; }
        }

        [Fact]
        public void WithGrouping()
        {
            //Arrange
            MemorySource<MyRow> source = new MemorySource<MyRow>();
            source.Data = new List<MyRow>()
                {
                new MyRow { Id = 1, ClassName = "Class1", DetailValue = 3.5 },
                new MyRow { Id = 2, ClassName = "Class1", DetailValue = 6.5 },
                new MyRow { Id = 3, ClassName = "Class2", DetailValue = 1.2 },
                new MyRow { Id = 4, ClassName = "Class2", DetailValue = 2.3 },
                new MyRow { Id = 5, ClassName = "Class2", DetailValue = 16.5 },
                new MyRow { Id = 6, ClassName = "Class3", DetailValue = 30.0 },
                new MyRow { Id = 6, ClassName = null, DetailValue = 14.5 },
                new MyRow { Id = 6, ClassName = null, DetailValue = 15.5 },
                };

            Aggregation<MyRow, MyAggRow> agg = new Aggregation<MyRow, MyAggRow>();

            MemoryDestination<MyAggRow> dest = new MemoryDestination<MyAggRow>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();


            //Assert
            Assert.Collection<MyAggRow>(dest.Data,
                ar => Assert.True(ar.AggValue == 10 && ar.GroupName == "Class1"),
                ar => Assert.True(ar.AggValue == 20 && ar.GroupName == "Class2"),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupName == "Class3"),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupName == null)
            );
        }

        public class MyRowNullable
        {
            public int Id { get; set; }
            [GroupColumn("GroupId")]
            public int? ClassId { get; set; }
            [AggregateColumn("AggValue", AggregationMethod.Sum)]
            public double? DetailValue { get; set; }
        }

        public class MyAggRowNullable
        {
            public int? GroupId { get; set; }
            public double? AggValue { get; set; }
        }

        [Fact]
        public void WithNullable()
        {
            //Arrange
            MemorySource<MyRowNullable> source = new MemorySource<MyRowNullable>();
            source.Data = new List<MyRowNullable>()
                {
                new MyRowNullable { Id = 1, ClassId = 1, DetailValue = 3.5 },
                new MyRowNullable { Id = 2, ClassId = 1, DetailValue = 6.5 },
                new MyRowNullable { Id = 3, ClassId = 2, DetailValue = 1.2 },
                new MyRowNullable { Id = 4, ClassId = 2, DetailValue = 2.3 },
                new MyRowNullable { Id = 5, ClassId = 2, DetailValue = 16.5 },
                new MyRowNullable { Id = 6, ClassId = 3, DetailValue = 30.0 },
                new MyRowNullable { Id = 6, ClassId = null, DetailValue = 14.5 },
                new MyRowNullable { Id = 6, ClassId = null, DetailValue = 15.5 },
                };

            Aggregation<MyRowNullable, MyAggRowNullable> agg = new Aggregation<MyRowNullable, MyAggRowNullable>();

            MemoryDestination<MyAggRowNullable> dest = new MemoryDestination<MyAggRowNullable>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();


            //Assert
            Assert.Collection<MyAggRowNullable>(dest.Data,
                ar => Assert.True(ar.AggValue == 10 && ar.GroupId == 1),
                ar => Assert.True(ar.AggValue == 20 && ar.GroupId == 2),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupId == 3),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupId == null)
            );
        }

        public class MyRowMultiple
        {
            public int Id { get; set; }
            [GroupColumn("Group1Name")]
            public string Class1Name { get; set; }
            [GroupColumn("Group2Name")]
            public string Class2Name { get; set; }
            [AggregateColumn("AggValue1", AggregationMethod.Sum)]
            public int DetailValue1 { get; set; }
            [AggregateColumn("AggValue2", AggregationMethod.Count)]
            public double DetailValue2 { get; set; }
        }

        public class MyAggRowMultiple
        {
            public string Group1Name { get; set; }
            public string Group2Name { get; set; }
            public double AggValue1 { get; set; }
            public int AggValue2 { get; set; }
        }

        [Fact]
        public void WithMultipleGroupingAndAggregation()
        {
            //Arrange
            MemorySource<MyRowMultiple> source = new MemorySource<MyRowMultiple>();
            source.Data = new List<MyRowMultiple>()
                {
                new MyRowMultiple { Id = 1, Class1Name = "Class", Class2Name = "1", DetailValue1 = 4 },
                new MyRowMultiple { Id = 2, Class1Name = "Class", Class2Name = "1", DetailValue1 = 6 },
                new MyRowMultiple { Id = 3, Class1Name = "Class2",Class2Name = null, DetailValue1 = 3 },
                new MyRowMultiple { Id = 4, Class1Name = "Class2",Class2Name = null, DetailValue1 = 7 },
                new MyRowMultiple { Id = 5, Class1Name = "Class",Class2Name = "3", DetailValue1 = 10 },
                };

            Aggregation<MyRowMultiple, MyAggRowMultiple> agg = new Aggregation<MyRowMultiple, MyAggRowMultiple>();

            MemoryDestination<MyAggRowMultiple> dest = new MemoryDestination<MyAggRowMultiple>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();


            //Assert
            Assert.Collection<MyAggRowMultiple>(dest.Data,
                ar => Assert.True(ar.AggValue1 == 10 && ar.AggValue2 == 2 && ar.Group1Name == "Class" && ar.Group2Name == "1"),
                ar => Assert.True(ar.AggValue1 == 10 && ar.AggValue2 == 2 && ar.Group1Name == "Class2" && ar.Group2Name == null),
                ar => Assert.True(ar.AggValue1 == 10 && ar.AggValue2 == 1 && ar.Group1Name == "Class" && ar.Group2Name == "3")
            );
        }
    }
}
