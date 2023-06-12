namespace TestFlatFileConnectors.CSVDestination
{
    public class CsvDestinationErrorLinkingTests
    {
        public class MySimpleRow
        {
            public string Col1 { get; set; }
            public string Col2
            {
                get
                {
                    if (Col1 is null or "X")
                        throw new Exception("Error record!");
                    return "Test" + Col1;
                }
            }
        }

        [Fact]
        public void RedirectSingleRecordWithObject()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>
            {
                DataAsList = new List<MySimpleRow>
                {
                    new() { Col1 = "X" },
                    new() { Col1 = "1" },
                    new() { Col1 = "2" },
                    new() { Col1 = null },
                    new() { Col1 = "3" }
                }
            };
            CsvDestination<MySimpleRow> dest = new CsvDestination<MySimpleRow>("ErrorFile.csv");
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            dest.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("./ErrorFile.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsErrorLinking.csv")
            );
            Assert.Collection(
                errorDest.Data,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    ),
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }

        [Fact]
        public void NoErrorHandling()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>
            {
                DataAsList = new List<MySimpleRow>
                {
                    new() { Col1 = "X" },
                    new() { Col1 = "1" },
                    new() { Col1 = null }
                }
            };
            CsvDestination<MySimpleRow> dest = new CsvDestination<MySimpleRow>(
                "ErrorFileNoError.csv"
            );

            //Act
            //Assert
            Assert.ThrowsAny<Exception>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }
    }
}
