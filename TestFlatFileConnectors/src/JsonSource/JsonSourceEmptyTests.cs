using ETLBox.DataFlow;
using ETLBox.DataFlow;
using Xunit;

namespace ETLBoxTests.DataFlowTests
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
