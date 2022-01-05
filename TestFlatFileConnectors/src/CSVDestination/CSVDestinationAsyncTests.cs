using System.IO;
using System.Threading.Tasks;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public sealed class CsvDestinationAsyncTests
    {
        [Theory]
        [InlineData("AsyncTestFile.csv", 5000)]
        public void WriteAsyncAndCheckLock(string filename, int noRecords)
        {
            //Arrange
            if (File.Exists(filename)) File.Delete(filename);
            var source = new MemorySource<string[]>();
            for (var i = 0; i < noRecords; i++)
                source.DataAsList.Add(new[] { HashHelper.RandomString(100) });
            var dest = new CsvDestination<string[]>(filename);
            var onCompletionRun = false;

            //Act
            source.LinkTo(dest);
            var sT = source.ExecuteAsync();
            var dt = dest.Completion;
            while (!File.Exists(filename)) Task.Delay(10).Wait();

            //Assert
            dest.OnCompletion = () =>
            {
                Assert.False(IsFileLocked(filename), "StreamWriter should be disposed and file unlocked");
                onCompletionRun = true;
            };

            Assert.True(IsFileLocked(filename), "Right after  start the file should still be locked.");
            dt.Wait();
            Assert.True(onCompletionRun, "OnCompletion action and assertion did run");
        }

        private bool IsFileLocked(string filename)
        {
            try
            {
                var file = new FileInfo(filename);
                using var stream = file.Open(FileMode.Open, FileAccess.Read, FileShare.None);
                stream.Close();
            }
            catch (IOException)
            {
                //the file is unavailable because it is:
                //still being written to
                //or being processed by another thread
                //or does not exist (has already been processed)
                return true;
            }

            //file is not locked
            return false;
        }
    }
}