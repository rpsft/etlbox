using ALE.ETLBox.src.Definitions.TaskBase.DataFlow;
using ALE.ETLBox.src.Toolbox.DataFlow;
using Xunit.Abstractions;

namespace TestFlatFileConnectors.src.CsvDestination
{
    public sealed class CsvDestinationAsyncTests
    {
        private readonly ITestOutputHelper _output;

        public CsvDestinationAsyncTests(ITestOutputHelper output)
        {
            _output = output;
        }

        private class MockDelaySource : DataFlowSource<string[]>
        {
            public override void Execute()
            {
                Buffer.SendAsync(new[] { "1", "2" }).Wait();
                Thread.Sleep(100);
                Buffer.SendAsync(new[] { "3", "4" }).Wait();
                Buffer.Complete();
            }
        }

        [Theory]
        [InlineData("AsyncTestFile.csv")]
        public void WriteAsyncAndCheckLock(string filename)
        {
            //Arrange
            if (File.Exists(filename))
                File.Delete(filename);
            var source = new MockDelaySource();
            var dest = new CsvDestination<string[]>(filename);
            var onCompletionRun = false;
            var fileWasLockedOnCompletion = false;
            Exception exceptionFileLockCheck = null;
            dest.OnCompletion = () =>
            {
                _output.WriteLine("OnCompletion invoked!");
                onCompletionRun = true;
                exceptionFileLockCheck = Record.Exception(
                    () => fileWasLockedOnCompletion = IsFileLocked(filename)
                );
            };

            //Act
            source.LinkTo(dest);
            source.ExecuteAsync();
            var dt = dest.Completion;
            while (!File.Exists(filename))
                Task.Delay(10).Wait();
            dt.Wait();

            //Assert
            Assert.Multiple(
                () => Assert.Null(exceptionFileLockCheck),
                () => Assert.True(onCompletionRun, "OnCompletion action and assertion did run"),
                () =>
                    Assert.False(
                        fileWasLockedOnCompletion,
                        "StreamWriter should be disposed and file unlocked"
                    )
            );
        }

        private static bool IsFileLocked(string filename)
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
