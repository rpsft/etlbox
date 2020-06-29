using CsvHelper.Configuration.Attributes;
using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Dynamic;
using System.IO;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class TextDestinationTests
    {
        public TextDestinationTests()
        {
        }

        public class MyTextRow
        {
            public string Text { get; set; }
        }

        [Fact]
        public void WriteWithObject()
        {
            //Arrange
            var source = new MemorySource<MyTextRow>();
            source.DataAsList.Add(new MyTextRow() { Text = "Line 1" });
            source.DataAsList.Add(new MyTextRow() { Text = "Line 2" });
            source.DataAsList.Add(new MyTextRow() { Text = "Line 3" });

            //Act
            TextDestination<MyTextRow> dest = new TextDestination<MyTextRow>("res/TextDestination/TestFile.txt"
                , tr => tr.Text);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(File.Exists("res/TextDestination/TestFile.txt"));
            Assert.Equal(File.ReadAllText("res/TextDestination/ToBeTestFile.txt"), File.ReadAllText("res/TextDestination/TestFile.txt"));
        }

        [Fact]
        public void WriteWithDynamicObject()
        {
            //Arrange
            var source = new MemorySource();
            dynamic n1 = new ExpandoObject(); n1.Text = "Line 1"; source.DataAsList.Add(n1);
            dynamic n2 = new ExpandoObject(); n2.Text = "Line 2"; source.DataAsList.Add(n2);
            dynamic n3 = new ExpandoObject(); n3.Text = "Line 3"; source.DataAsList.Add(n3);

            //Act
            TextDestination dest = new TextDestination("res/TextDestination/TestFileDynamic.txt"
                , tr => {
                    dynamic r = tr as ExpandoObject;
                    return r.Text;
                });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(File.Exists("res/TextDestination/TestFileDynamic.txt"));
            Assert.Equal(File.ReadAllText("res/TextDestination/ToBeTestFile.txt"), File.ReadAllText("res/TextDestination/TestFileDynamic.txt"));
        }

        [Fact]
        public void WriteWithArray()
        {
            //Arrange
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(new string[] { "Line 1" });
            source.DataAsList.Add(new string[] { "Line 2" });
            source.DataAsList.Add(new string[] { "Line 3" });

            //Act
            TextDestination<string[]> dest = new TextDestination<string[]>("res/TextDestination/TestFileArray.txt"
                , tr => tr[0]);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(File.Exists("res/TextDestination/TestFileArray.txt"));
            Assert.Equal(File.ReadAllText("res/TextDestination/ToBeTestFile.txt"), File.ReadAllText("res/TextDestination/TestFileArray.txt"));
        }
    }
}
