using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestFlatFileConnectors.src;
using TestFlatFileConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestFlatFileConnectors.src.XmlSource
{
    public class XmlSourceTests : FlatFileConnectorsTestBase
    {
        public XmlSourceTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void XmlOnlyElements()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture("XmlSource2Cols");
            var dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "XmlSource2Cols"
            );

            //Act
            var source = new XmlSource<MySimpleRow>(
                "res/XmlSource/TwoColumnsOnlyElements.xml",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [XmlRoot("MySimpleRow")]
        public class MyAttributeRow
        {
            [XmlAttribute]
            public int Col1 { get; set; }

            [XmlAttribute]
            public string Col2 { get; set; }
        }

        [Fact]
        public void XmlOnlyAttributes()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "XmlSource2ColsAttribute"
            );
            var dest = new DbDestination<MyAttributeRow>(
                SqlConnection,
                "XmlSource2ColsAttribute"
            );

            //Actt
            var source = new XmlSource<MyAttributeRow>(
                "res/XmlSource/TwoColumnsOnlyAttributes.xml",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
