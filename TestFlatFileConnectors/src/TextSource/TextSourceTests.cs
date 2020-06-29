using CsvHelper.Configuration.Attributes;
using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Dynamic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class TextSourceTests
    {
        public TextSourceTests()
        {
        }

        public class MyTextRow
        {

            public string Text { get; set; }
        }

        [Fact]
        public void ReadingIntoObject()
        {
            //Arrange
            var dest = new MemoryDestination<MyTextRow>();

            //Act
            TextSource<MyTextRow> source = new TextSource<MyTextRow>();
            source.Uri = "res/TextSource/Test.txt";
            source.WriteLineIntoObject = (line, o) => o.Text = line;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(dest.Data.Count == 3);
            Assert.Collection<MyTextRow>(dest.Data,
                r => Assert.Equal("Line 1", r.Text),
                r => Assert.Equal("Line 2", r.Text),
                r => Assert.Equal("Line 3", r.Text)
            );
        }

        [Fact]
        public void ReadingIntoDynamicObject()
        {
            //Arrange
            var dest = new MemoryDestination();

            //Act
            TextSource source = new TextSource();
            source.Uri = "res/TextSource/Test.txt";
            source.WriteLineIntoObject = (line, dynob) =>
            {
                dynamic o = dynob as ExpandoObject;
                o.Text = line;
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(dest.Data.Count == 3);
            Assert.Collection<ExpandoObject>(dest.Data,
                row => { dynamic r = row as ExpandoObject; Assert.Equal("Line 1", r.Text); },
                row => { dynamic r = row as ExpandoObject; Assert.Equal("Line 2", r.Text); },
                row => { dynamic r = row as ExpandoObject; Assert.Equal("Line 3", r.Text); }
            );
        }

        [Fact]
        public void ReadingIntoStringArray()
        {
            //Arrange
            var dest = new MemoryDestination<string[]>();

            //Act
            TextSource<string[]> source = new TextSource<string[]>();
            source.Uri = "res/TextSource/Test.txt";
            source.WriteLineIntoObject = (line, o) => o[0] = line;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(dest.Data.Count == 3);
            Assert.Collection<string[]>(dest.Data,
               r => Assert.Equal("Line 1", r[0]),
               r => Assert.Equal("Line 2", r[0]),
               r => Assert.Equal("Line 3", r[0])
           );
        }
    }
}
