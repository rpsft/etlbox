using System.IO;
using System.Xml.Serialization;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestFlatFileConnectors.Helpers;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.XmlDestination
{
    [Collection("DataFlow")]
    public class XmlDestinationTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("XmlDestSimple");
            s2C.InsertTestDataSet3();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "XmlDestSimple"
            );

            //Act
            XmlDestination<MySimpleRow> dest = new XmlDestination<MySimpleRow>(
                "./SimpleWithObject.xml",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("res/XmlDestination/TwoColumnsSet3.xml").NormalizeLineEndings(),
                File.ReadAllText("./SimpleWithObject.xml")
            );
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
        public void SimpleOnlyAttributes()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("XmlDestOnlyAttributes");
            s2C.InsertTestDataSet3();
            DbSource<MyAttributeRow> source = new DbSource<MyAttributeRow>(
                SqlConnection,
                "XmlDestOnlyAttributes"
            );

            //Act
            XmlDestination<MyAttributeRow> dest = new XmlDestination<MyAttributeRow>(
                "./SimpleOnlyAttributes.xml",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("res/XmlDestination/TwoColumnsAttributesSet3.xml")
                    .NormalizeLineEndings(),
                File.ReadAllText("./SimpleOnlyAttributes.xml")
            );
        }
    }
}
