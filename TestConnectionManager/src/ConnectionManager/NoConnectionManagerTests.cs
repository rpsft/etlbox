using ALE.ETLBox;
using ALE.ETLBox.DataFlow;

namespace TestConnectionManager.src.ConnectionManager
{
    public class NoConnectionManagerTests
    {
        [Fact]
        public void DbSource()
        {
            //Arrange
            var source = new DbSource<string[]>("test");
            var dest = new MemoryDestination<string[]>();
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }

        [Fact]
        public void DbDestination()
        {
            //Arrange
            string[] data = { "1", "2" };
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(data);
            var dest = new DbDestination<string[]>("test");
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                try
                {
                    source.Execute();
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException!;
                }
            });
        }
    }
}
