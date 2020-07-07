using ETLBox.Helper;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace ETLBoxTests
{
    public class TPLDataFlowTests
    {
        [Fact]
        public void TPLDataflowIsBehavingWeird()
        {
            //Arrange
            var total = 10;
            var processed = 0;

            //BroadcastBlock does not send if bounded capacity is set and buffer in target is full
            //Message will be "lost"
            BroadcastBlock<int> bb = new BroadcastBlock<int>( c=> c);
            ActionBlock<int> ab = new ActionBlock<int>(
               (messageUnit) =>
               {
                   Thread.Sleep(10);
                   processed++;
               },
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 2 }
           );

            bb.LinkTo(ab, new DataflowLinkOptions() { PropagateCompletion = true });

            //Act
            for (int i = 0; i < total; i++)
                bb.SendAsync(i).Wait();

            bb.Complete();
            ab.Completion.Wait();

            //Assert
            Assert.NotEqual(total, processed); //Should be Assert.Equal(total, processed) if BroadcastBlock would wait!
        }

    }
}
