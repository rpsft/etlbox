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
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class LookupAttributeTests
    {
        public LookupAttributeTests()
        {
        }

        public class LookupData
        {
            public int Id { get; set; }
            public string Value { get; set; }
        }

        public class InputDataRow
        {
            public int Col1 { get; set; }
            [MatchColumn("Id")]
            public int LookupId => Col1;
            [RetrieveColumn("Value")]
            public string LookupValue { get; set; }
        }


        [Fact]
        public void OneMatchOneRetrieveColumn()
        {
            //Arrange
            MemorySource<InputDataRow> source = new MemorySource<InputDataRow>();
            source.Data.Add(new InputDataRow() { Col1 = 1 });
            source.Data.Add(new InputDataRow() { Col1 = 2 });
            source.Data.Add(new InputDataRow() { Col1 = 4 });
            source.Data.Add(new InputDataRow() { Col1 = 3 });
            MemorySource<LookupData> lookupSource = new MemorySource<LookupData>();
            lookupSource.Data.Add(new LookupData() { Id = 1, Value = "Test1" });
            lookupSource.Data.Add(new LookupData() { Id = 2, Value = "Test2" });
            lookupSource.Data.Add(new LookupData() { Id = 3, Value = "Test3" });

            var lookup = new LookupTransformation<InputDataRow, LookupData>();
            lookup.Source = lookupSource;
            MemoryDestination<InputDataRow> dest = new MemoryDestination<InputDataRow>();
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<InputDataRow>(dest.Data,
                r => Assert.True(r.LookupId == 1 && r.LookupValue == "Test1"),
                r => Assert.True(r.LookupId == 2 && r.LookupValue == "Test2"),
                r => Assert.True(r.LookupId == 4 && r.LookupValue == null),
                r => Assert.True(r.LookupId == 3 && r.LookupValue == "Test3")
                );
        }

        public class LookupDataMultiple
        {
            public int Id1 { get; set; }
            public string Id2 { get; set; }
            public string Value1 { get; set; }
            public int Value2 { get; set; }
        }

        public class InputDataMultiple
        {
            [MatchColumn("Id1")]
            public int LookupId1 { get; set; }
            [MatchColumn("Id2")]
            public string LookupId2 { get; set; }
            [RetrieveColumn("Value1")]
            public string LookupValue1 { get; set; }
            [RetrieveColumn("Value2")]
            public int LookupValue2 { get; set; }
        }

        [Fact]
        public void MultipleMatchAndRetrieveColumns()
        {
            //Arrange
            MemorySource<InputDataMultiple> source = new MemorySource<InputDataMultiple>();
            source.Data.Add(new InputDataMultiple() { LookupId1 = 1, LookupId2 = "T1" });
            source.Data.Add(new InputDataMultiple() { LookupId1 = 2, LookupId2 = "TX" });
            source.Data.Add(new InputDataMultiple() { LookupId1 = 4, LookupId2 = "T2" });
            source.Data.Add(new InputDataMultiple() { LookupId1 = 3, LookupId2 = "T3" });
            MemorySource<LookupDataMultiple> lookupSource = new MemorySource<LookupDataMultiple>();
            lookupSource.Data.Add(new LookupDataMultiple() { Id1 = 1, Id2 = "T1", Value1 = "Test1", Value2 = 100 });
            lookupSource.Data.Add(new LookupDataMultiple() { Id1 = 2, Value1 = "Test2", Value2 = 200 });
            lookupSource.Data.Add(new LookupDataMultiple() { Id1 = 3, Id2 = "T3", Value2 = 300 });

            var lookup = new LookupTransformation<InputDataMultiple, LookupDataMultiple>();
            lookup.Source = lookupSource;
            MemoryDestination<InputDataMultiple> dest = new MemoryDestination<InputDataMultiple>();
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<InputDataMultiple>(dest.Data,
                r => Assert.True(r.LookupId1 == 1 && r.LookupValue1 == "Test1" && r.LookupValue2 == 100),
                r => Assert.True(r.LookupId1 == 2 && r.LookupValue1 == null),
                r => Assert.True(r.LookupId1 == 4 && r.LookupValue1 == null),
                r => Assert.True(r.LookupId1 == 3 && r.LookupValue1 == null && r.LookupValue2 == 300)
                );
        }

        public class InputDataError1
        {
            [MatchColumn("Id")]
            public int LookupId { get; set; }
        }

        public class InputDataError2
        {
            [MatchColumn("XXX")]
            public int LookupId { get; set; }
            [RetrieveColumn("Value")]
            public string LookupValue { get; set; }
        }

        public class InputDataError3
        {
            [MatchColumn("Id")]
            public int LookupId { get; set; }
            [RetrieveColumn("XXX")]
            public string LookupValue { get; set; }
        }


        [Fact]
        public void TestExceptions()
        {
            RunExceptionFlowWithType<InputDataError1>(new InputDataError1() { LookupId = 1 });
            RunExceptionFlowWithType<InputDataError2>(new InputDataError2() { LookupId = 1 });
            RunExceptionFlowWithType<InputDataError3>(new InputDataError3() { LookupId = 1 });

        }

        private static void RunExceptionFlowWithType<T>(T sourceDataItem)
        {
            //Arrange
            MemorySource<T> source = new MemorySource<T>();
            source.Data.Add(sourceDataItem);
            MemorySource<LookupData> lookupSource = new MemorySource<LookupData>();
            var lookup = new LookupTransformation<T, LookupData>(lookupSource);
            MemoryDestination<T> dest = new MemoryDestination<T>();

            source.LinkTo(lookup);
            lookup.LinkTo(dest);

            //Act && Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                try
                {
                    source.Execute();
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            });
        }
    }
}

