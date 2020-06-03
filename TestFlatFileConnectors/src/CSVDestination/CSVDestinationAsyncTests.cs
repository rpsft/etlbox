using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Helper;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvDestinationAsyncTests
    {
        [Theory, InlineData("AsyncTestFile.csv", 5000)]
        public void WriteAsyncAndCheckLock(string filename, int noRecords)
        {
            //Arrange
            if (File.Exists(filename)) File.Delete(filename);
            MemorySource<string[]> source = new MemorySource<string[]>();
            for (int i = 0; i < noRecords; i++)
                source.DataAsList.Add(new string[] { TestHashHelper.RandomString(100) });
            CsvDestination<string[]> dest = new CsvDestination<string[]>(filename);
            bool onCompletionRun = false;

            //Act
            source.LinkTo(dest);
            Task sT = source.ExecuteAsync();
            Task dt = dest.Completion;
            while (!File.Exists(filename)) { Task.Delay(10).Wait(); }

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

        protected virtual bool IsFileLocked(string filename)
        {
            try
            {
                FileInfo file = new FileInfo(filename);
                using (FileStream stream = file.Open(FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    stream.Close();
                }
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
