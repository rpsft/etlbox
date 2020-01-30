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
using System.Dynamic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class LookupAttributeDynamicTests
    {
        public LookupAttributeDynamicTests()
        {
        }

        public class LookupData
        {
            public int Id { get; set; }
            public string Value { get; set; }
        }


        [Fact]
        public void OneMatchOneRetrieveColumn()
        {
            //Arrange
            MemorySource<ExpandoObject> source = new MemorySource<ExpandoObject>();
            dynamic one = new ExpandoObject();
            one.Id = 1;
            one.Value = "";
            source.Data.Add(one);
            dynamic two = new ExpandoObject();
            one.Id = 1;
            one.Value = null;
            source.Data.Add(two);
            MemorySource<LookupData> lookupSource = new MemorySource<LookupData>();
            lookupSource.Data.Add(new LookupData() { Id = 1, Value = "Test1" });
            lookupSource.Data.Add(new LookupData() { Id = 2, Value = "Test2" });
            //lookupSource.Data.Add(new LookupData() { Id = 3, Value = "Test3" });

            var lookup = new LookupTransformation<ExpandoObject, LookupData>();
            lookup.Source = lookupSource;
            MemoryDestination<ExpandoObject> dest = new MemoryDestination<ExpandoObject>();
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //Assert.Collection<ExpandoObject>(dest.Data,
            //    r => Assert.True(r.LookupId == 1 && r.LookupValue == "Test1"),
            //    r => Assert.True(r.LookupId == 2 && r.LookupValue == "Test2"),
            //    r => Assert.True(r.LookupId == 4 && r.LookupValue == null),
            //    r => Assert.True(r.LookupId == 3 && r.LookupValue == "Test3")
            //    );
        }


    }
}

