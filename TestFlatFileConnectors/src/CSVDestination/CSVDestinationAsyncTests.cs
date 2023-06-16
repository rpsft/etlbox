namespace TestFlatFileConnectors.CSVDestination
{
    public sealed class CsvDestinationAsyncTests
    {
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

            //Act
            source.LinkTo(dest);
            source.ExecuteAsync();
            var dt = dest.Completion;
            while (!File.Exists(filename))
                Task.Delay(10).Wait();

            //Assert
            dest.OnCompletion = () =>
            {
                Assert.False(
                    IsFileLocked(filename),
                    "StreamWriter should be disposed and file unlocked"
                );
                onCompletionRun = true;
            };

            Assert.True(
                IsFileLocked(filename),
                "Right after  start the file should still be locked."
            );
            dt.Wait();
            Assert.True(onCompletionRun, "OnCompletion action and assertion did run");
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
