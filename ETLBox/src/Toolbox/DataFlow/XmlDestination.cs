using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Linq;
using System.Xml.Serialization;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A Xml destination defines a xml file where data from the flow is inserted. 
    /// </summary>
    /// <see cref="XmlDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// XmlDestination&lt;MyRow&gt; dest = new XmlDestination&lt;MyRow&gt;("/path/to/file.json");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class XmlDestination<TInput> : DataFlowStreamDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write Xml into file {Uri ?? ""}";

        public string RootElementName { get; set; } = "Root";
        public string DynamicElementName { get; set; } 
        public XmlWriter XmlWriter { get; set; }
        public XmlWriterSettings Settings { get; set; } = new XmlWriterSettings()
        {
            Indent = true,
            NamespaceHandling = NamespaceHandling.OmitDuplicates
        };

        XmlSerializer XmlSerializer { get; set; }
        XmlTypeInfo TypeInfo { get; set; }
        XmlSerializerNamespaces NS { get; set; }

        public XmlDestination() : base()
        {
            TypeInfo = new XmlTypeInfo(typeof(TInput));
            if (!TypeInfo.IsDynamic)
                XmlSerializer = new XmlSerializer(typeof(TInput), "");
            InitTargetAction();
        }

        public XmlDestination(string fileName) : this()
        {
            Uri = fileName;
        }

        public XmlDestination(string uri, ResourceType resourceType) : this(uri)
        {
            ResourceType = resourceType;
        }

        protected override void InitStream()
        {
            NS = new XmlSerializerNamespaces();
            NS.Add("", "");
            XmlWriter = XmlWriter.Create(StreamWriter, Settings);
            XmlWriter.WriteStartDocument();
            XmlWriter.WriteStartElement(RootElementName);
        }

        protected override void WriteIntoStream(TInput data)
        {
            if (data == null) return;
            try
            {
                if (TypeInfo.IsDynamic)
                {
                    string json = JsonConvert.SerializeObject(data);
                    XDocument doc = JsonConvert.DeserializeXNode(json, DynamicElementName ?? "Dynamic") as XDocument;
                    doc.Root.WriteTo(XmlWriter);
                }
                else
                {
                    XmlSerializer.Serialize(XmlWriter, data, NS);
                }
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(data));
            }
            LogProgress();
        }

        protected override void CloseStream()
        {
            XmlWriter?.WriteEndElement();
            XmlWriter?.Flush();
            XmlWriter?.Close();
        }
    }

    /// <summary>
    /// A Xml destination defines a Xml file where data from the flow is inserted. 
    /// The XmlDestination uses a dynamic object as input type. If you need other data types, use the generic CsvDestination instead.
    /// </summary>
    /// <see cref="XmlDestination{TInput}"/>
    public class XmlDestination : XmlDestination<ExpandoObject>
    {
        public XmlDestination() : base() { }

        public XmlDestination(string fileName) : base(fileName) { }

    }

}
