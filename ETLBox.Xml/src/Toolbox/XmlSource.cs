using ETLBox.DataFlow;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;
using TheBoxOffice.LicenseManager;

namespace ETLBox.Xml
{
    /// <summary>
    /// Reads data from a xml source. This can be any http resource or a file.
    /// By default, data is pulled via httpclient. Use the ResourceType property to read data from a file.
    /// </summary>
    /// <example>
    /// <code>
    /// XmlSource&lt;POCO&gt; source = new XmlSource&lt;POCO&gt;("https://jsonplaceholder.typicode.com/todos");
    /// </code>
    /// </example>
    public class XmlSource<TOutput> : DataFlowStreamSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read Xml from Uri: {CurrentRequestUri ?? ""}";

        /// <summary>
        /// The XmlSerializer used to deserialize the xml into the used data type.
        /// </summary>
        public XmlSerializer XmlSerializer { get; set; }
        public string ElementName { get; set; }

        /// <summary>
        /// The element name of the document that contains an item of the data to be parsed
        /// </summary>

        /* Private stuff */
        XmlReader XmlReader { get; set; }

        XmlTypeInfo TypeInfo { get; set; }

        public XmlSource()
        {
            TypeInfo = new XmlTypeInfo(typeof(TOutput));
            if (!TypeInfo.IsDynamic)
                XmlSerializer = new XmlSerializer(typeof(TOutput));
        }

        public XmlSource(string uri) : this()
        {
            Uri = uri;
        }

        public XmlSource(string uri, ResourceType resourceType) : this(uri)
        {
            ResourceType = resourceType;
        }

        protected override void InitReader()
        {
            XmlReader = XmlReader.Create(StreamReader);
        }

        protected override void ReadAll()
        {

            XmlReader.MoveToContent();
            while (XmlReader.Read())
            {
                if (XmlReader.NodeType == XmlNodeType.Element)
                {
                    if (XmlReader.Name == (ElementName ?? TypeInfo.ElementName))
                    {
                        XElement el = XNode.ReadFrom(XmlReader) as XElement;
                        if (el != null)
                        {
                            try
                            {
                                TOutput output = default(TOutput);
                                if (TypeInfo.IsDynamic)
                                {
                                    string jsonText = JsonConvert.SerializeXNode(el);
                                    dynamic res = JsonConvert.DeserializeObject<ExpandoObject>(jsonText) as dynamic;
                                    output = ((IDictionary<string, object>)res)[ElementName] as dynamic;
                                }
                                else
                                {
                                    output = (TOutput)XmlSerializer.Deserialize(el.CreateReader());
                                }
                                Buffer.SendAsync(output).Wait();
                            }
                            catch (Exception e)
                            {
                                if (!ErrorHandler.HasErrorBuffer) throw e;
                                ErrorHandler.Send(e, el.ToString());
                            }
                            LogProgress();
                            if (ProgressCount > 0 && ProgressCount % LicenseCheck.FreeRows == 0)
                                LicenseCheck.CheckValidLicenseOrThrow();
                        }
                    }
                }
            }
        }

        protected override void CloseReader()
        {
            XmlReader?.Close();
        }
    }

    /// <summary>
    /// Reads data from a xml source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// XmlSource as a nongeneric type returns a dynamic object as output. If you need typed output, use
    /// the XmlSource&lt;TOutput&gt; object instead.
    /// </summary>
    /// <see cref="XmlSource{TOutput}"/>
    /// <example>
    /// <code>
    /// XmlSource source = new XmlSource("demodata.json");
    /// source.LinkTo(dest); //Link to transformation or destination
    /// source.Execute(); //Start the dataflow
    /// </code>
    /// </example>
    public class XmlSource : XmlSource<ExpandoObject>
    {
        public XmlSource() : base() { }
        public XmlSource(string uri) : base(uri) { }
        public XmlSource(string uri, ResourceType resourceType) : base(uri, resourceType) { }
    }
}
