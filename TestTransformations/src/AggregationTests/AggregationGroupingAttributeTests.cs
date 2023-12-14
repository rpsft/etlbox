using System.Diagnostics.CodeAnalysis;
using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;

namespace TestTransformations.src.AggregationTests
{
    [SuppressMessage("ReSharper", "CompareOfFloatsByEqualityOperator")]
    public class AggregationGroupingAttributeTests
    {
        [Serializable]
        public class MyRow
        {
            public int Id { get; set; }

            public string ClassName { get; set; }

            public double DetailValue { get; set; }
        }

        [Serializable]
        public class MyAggRow
        {
            [GroupColumn(nameof(MyRow.ClassName))]
            public string GroupName { get; set; }

            [AggregateColumn(nameof(MyRow.DetailValue), AggregationMethod.Sum)]
            public double AggValue { get; set; }
        }

        [Fact]
        public void WithGrouping()
        {
            //Arrange
            var source = new MemorySource<MyRow>
            {
                DataAsList = new List<MyRow>
                {
                    new()
                    {
                        Id = 1,
                        ClassName = "Class1",
                        DetailValue = 3.5
                    },
                    new()
                    {
                        Id = 2,
                        ClassName = "Class1",
                        DetailValue = 6.5
                    },
                    new()
                    {
                        Id = 3,
                        ClassName = "Class2",
                        DetailValue = 1.2
                    },
                    new()
                    {
                        Id = 4,
                        ClassName = "Class2",
                        DetailValue = 2.3
                    },
                    new()
                    {
                        Id = 5,
                        ClassName = "Class2",
                        DetailValue = 16.5
                    },
                    new()
                    {
                        Id = 6,
                        ClassName = "Class3",
                        DetailValue = 30.0
                    },
                    new()
                    {
                        Id = 6,
                        ClassName = null,
                        DetailValue = 14.5
                    },
                    new()
                    {
                        Id = 6,
                        ClassName = null,
                        DetailValue = 15.5
                    }
                }
            };

            var agg = new Aggregation<MyRow, MyAggRow>();

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
                ar => Assert.True(ar.AggValue == 20 && ar.GroupName == "Class2"),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupName == "Class3"),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupName == null)
            );
        }

        [Serializable]
        private class MyRowNullable
        {
            public int Id { get; set; }
            public int? ClassId { get; set; }
            public double? DetailValue { get; set; }
        }

        [Serializable]
        private class MyAggRowNullable
        {
            [GroupColumn("ClassId")]
            public int? GroupId { get; set; }

            [AggregateColumn("DetailValue", AggregationMethod.Sum)]
            public double? AggValue { get; set; }
        }

        [Fact]
        public void WithNullable()
        {
            //Arrange
            var source = new MemorySource<MyRowNullable>
            {
                DataAsList = new List<MyRowNullable>
                {
                    new()
                    {
                        Id = 1,
                        ClassId = 1,
                        DetailValue = 3.5
                    },
                    new()
                    {
                        Id = 2,
                        ClassId = 1,
                        DetailValue = 6.5
                    },
                    new()
                    {
                        Id = 3,
                        ClassId = 2,
                        DetailValue = 1.2
                    },
                    new()
                    {
                        Id = 4,
                        ClassId = 2,
                        DetailValue = 2.3
                    },
                    new()
                    {
                        Id = 5,
                        ClassId = 2,
                        DetailValue = 16.5
                    },
                    new()
                    {
                        Id = 6,
                        ClassId = 3,
                        DetailValue = 30.0
                    },
                    new()
                    {
                        Id = 6,
                        ClassId = null,
                        DetailValue = 14.5
                    },
                    new()
                    {
                        Id = 6,
                        ClassId = null,
                        DetailValue = 15.5
                    }
                }
            };

            var agg =
                new Aggregation<MyRowNullable, MyAggRowNullable>();

            var dest = new MemoryDestination<MyAggRowNullable>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                ar => Assert.True(ar.AggValue == 10 && ar.GroupId == 1),
                ar => Assert.True(ar.AggValue == 20 && ar.GroupId == 2),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupId == 3),
                ar => Assert.True(ar.AggValue == 30 && ar.GroupId == null)
            );
        }

        [Serializable]
        public class MyRowMultiple
        {
            public int Id { get; set; }
            public string Class1Name { get; set; }
            public string Class2Name { get; set; }
            public int DetailValue1 { get; set; }
            public double DetailValue2 { get; set; }
        }

        [Serializable]
        public class MyAggRowMultiple
        {
            [GroupColumn("Class1Name")]
            public string Group1Name { get; set; }

            [GroupColumn("Class2Name")]
            public string Group2Name { get; set; }

            [AggregateColumn("DetailValue1", AggregationMethod.Sum)]
            public double AggValue1 { get; set; }

            [AggregateColumn("DetailValue2", AggregationMethod.Count)]
            public int AggValue2 { get; set; }
        }

        [Fact]
        public void WithMultipleGroupingAndAggregation()
        {
            //Arrange
            var source = new MemorySource<MyRowMultiple>
            {
                DataAsList = new List<MyRowMultiple>
                {
                    new()
                    {
                        Id = 1,
                        Class1Name = "Class",
                        Class2Name = "1",
                        DetailValue1 = 4
                    },
                    new()
                    {
                        Id = 2,
                        Class1Name = "Class",
                        Class2Name = "1",
                        DetailValue1 = 6
                    },
                    new()
                    {
                        Id = 3,
                        Class1Name = "Class2",
                        Class2Name = null,
                        DetailValue1 = 3
                    },
                    new()
                    {
                        Id = 4,
                        Class1Name = "Class2",
                        Class2Name = null,
                        DetailValue1 = 7
                    },
                    new()
                    {
                        Id = 5,
                        Class1Name = "Class",
                        Class2Name = "3",
                        DetailValue1 = 10
                    }
                }
            };

            var agg =
                new Aggregation<MyRowMultiple, MyAggRowMultiple>();

            var dest = new MemoryDestination<MyAggRowMultiple>();

            //Act
            source.LinkTo(agg);
            agg.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                ar =>
                    Assert.True(
                        ar.AggValue1 == 10
                            && ar.AggValue2 == 2
                            && ar.Group1Name == "Class"
                            && ar.Group2Name == "1"
                    ),
                ar =>
                    Assert.True(
                        ar.AggValue1 == 10
                            && ar.AggValue2 == 2
                            && ar.Group1Name == "Class2"
                            && ar.Group2Name == null
                    ),
                ar =>
                    Assert.True(
                        ar.AggValue1 == 10
                            && ar.AggValue2 == 1
                            && ar.Group1Name == "Class"
                            && ar.Group2Name == "3"
                    )
            );
        }
    }
}
