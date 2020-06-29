using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using ETLBox.DataFlow;
using MySqlX.XDevAPI;
using System.Dynamic;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class XmlValidationTests
    {
        public XmlValidationTests()
        {
        }

        string xsdMarkup =
    @"<xsd:schema xmlns:xsd='http://www.w3.org/2001/XMLSchema'>  
       <xsd:element name='Root'>  
        <xsd:complexType>  
         <xsd:sequence>  
          <xsd:element name='Child1' minOccurs='1' maxOccurs='1'/>  
          <xsd:element name='Child2' minOccurs='1' maxOccurs='1'/>  
         </xsd:sequence>  
        </xsd:complexType>  
       </xsd:element>  
      </xsd:schema>";

        public class MyXmlRow
        {
            public string Xml { get; set; }
        }

        string _validXml => $@"<?xml version=""1.0"" encoding=""utf-8""?>
<Root>
    <Child1>Content1</Child1>
    <Child2>Content2</Child2>
</Root>
";
        string _invalidXml => $@"<?xml version=""1.0"" encoding=""utf-8""?>
<Root>
    <Child1>Content1</Child1>
    <Child3>Content3</Child3>
</Root>
";

        [Fact]
        public void ValidateSchemaForEachRow()
        {
            //Arrange
            var source = new MemorySource<MyXmlRow>();
            source.DataAsList.Add(new MyXmlRow() { Xml = _validXml });
            source.DataAsList.Add(new MyXmlRow() { Xml = _invalidXml });
            source.DataAsList.Add(new MyXmlRow() { Xml = _validXml });

            MemoryDestination<MyXmlRow> dest = new MemoryDestination<MyXmlRow>();
            MemoryDestination<ETLBoxError> error = new MemoryDestination<ETLBoxError>();

            //Act
            XmlSchemaValidation<MyXmlRow> schemaValidation = new XmlSchemaValidation<MyXmlRow>();
            schemaValidation.XmlSelector = r => r.Xml;
            schemaValidation.XmlSchema = xsdMarkup;

            source.LinkTo(schemaValidation);
            schemaValidation.LinkTo(dest);
            schemaValidation.LinkErrorTo(error);
            source.Execute();
            dest.Wait();
            error.Wait();

            //Assert
            Assert.True(dest.Data.Count == 2);
            Assert.True(error.Data.Count == 1);

        }

        [Fact]
        public void ValidateSchemaForDynamicObject()
        {
            //Arrange
            var source = new MemorySource();
            dynamic n1 = new ExpandoObject(); n1.Xml = _validXml; source.DataAsList.Add(n1);
            dynamic n2 = new ExpandoObject(); n2.Xml = _validXml; source.DataAsList.Add(n2);
            dynamic n3 = new ExpandoObject(); n3.Xml = _invalidXml; source.DataAsList.Add(n3);

            MemoryDestination dest = new MemoryDestination();
            MemoryDestination<ETLBoxError> error = new MemoryDestination<ETLBoxError>();

            //Act
            XmlSchemaValidation schemaValidation = new XmlSchemaValidation();
            schemaValidation.XmlSelector = row => {
                dynamic r = row as ExpandoObject;
                return r.Xml;
            };
            schemaValidation.XmlSchema = xsdMarkup;

            source.LinkTo(schemaValidation);
            schemaValidation.LinkTo(dest);
            schemaValidation.LinkErrorTo(error);
            source.Execute();
            dest.Wait();
            error.Wait();

            //Assert
            Assert.True(dest.Data.Count == 2);
            Assert.True(error.Data.Count == 1);

        }

        [Fact]
        public void ValidateSchemaForArray()
        {
            //Arrange
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(new string[] { _validXml });
            source.DataAsList.Add(new string[] { _invalidXml });
            source.DataAsList.Add(new string[] { _validXml });

            MemoryDestination<string[]> dest = new MemoryDestination<string[]>();
            MemoryDestination<ETLBoxError> error = new MemoryDestination<ETLBoxError>();

            //Act
            XmlSchemaValidation<string[]> schemaValidation = new XmlSchemaValidation<string[]>();
            schemaValidation.XmlSelector = row => row[0];
            schemaValidation.XmlSchema = xsdMarkup;

            source.LinkTo(schemaValidation);
            schemaValidation.LinkTo(dest);
            schemaValidation.LinkErrorTo(error);
            source.Execute();
            dest.Wait();
            error.Wait();

            //Assert
            Assert.True(dest.Data.Count == 2);
            Assert.True(error.Data.Count == 1);

        }
    }
}
