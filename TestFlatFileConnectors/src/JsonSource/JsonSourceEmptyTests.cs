using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;

namespace TestFlatFileConnectors.src.JsonSource
{
    public class JsonSourceEmptyTests
    {
        [Fact]
        public void ReadEmptyArray()
        {
            ALE.ETLBox.src.Toolbox.DataFlow.JsonSource source = new ALE.ETLBox.src.Toolbox.DataFlow.JsonSource(
                "res/JsonSource/EmptyArray.json",
                ResourceType.File
            );
            var dest = new MemoryDestination();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.True(dest.Data.Count == 0);
        }

        [Fact]
        public void JsonFromWebService()
        {
            ALE.ETLBox.src.Toolbox.DataFlow.JsonSource source = new ALE.ETLBox.src.Toolbox.DataFlow.JsonSource(
                "res/JsonSource/EmptyObject.json",
                ResourceType.File
            );
            var dest = new MemoryDestination();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.True(dest.Data.Count == 0);
        }
    }
}
