namespace TestFlatFileConnectors.JsonSource
{
    public class JsonSourceEmptyTests
    {
        [Fact]
        public void ReadEmptyArray()
        {
            ALE.ETLBox.DataFlow.JsonSource source = new ALE.ETLBox.DataFlow.JsonSource(
                "res/JsonSource/EmptyArray.json",
                ResourceType.File
            );
            MemoryDestination dest = new MemoryDestination();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.True(dest.Data.Count == 0);
        }

        [Fact]
        public void JsonFromWebService()
        {
            ALE.ETLBox.DataFlow.JsonSource source = new ALE.ETLBox.DataFlow.JsonSource(
                "res/JsonSource/EmptyObject.json",
                ResourceType.File
            );
            MemoryDestination dest = new MemoryDestination();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.True(dest.Data.Count == 0);
        }
    }
}
