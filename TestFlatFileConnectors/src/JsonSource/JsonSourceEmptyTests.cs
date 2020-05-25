using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceEmptyTests
    {
        [Fact]
        public void ReadEmptyArray()
        {
            JsonSource source = new JsonSource("res/JsonSource/EmptyArray.json", ResourceType.File);
            MemoryDestination dest = new MemoryDestination();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.True(dest.Data.Count == 0);
        }

        [Fact]
        public void JsonFromWebService()
        {
            JsonSource source = new JsonSource("res/JsonSource/EmptyObject.json", ResourceType.File);
            MemoryDestination dest = new MemoryDestination();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.True(dest.Data.Count == 0);
        }

    }
}
