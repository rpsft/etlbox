using Newtonsoft.Json;
using System;
using System.Dynamic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Reads data from a json source. This can be any http resource or a file.
    /// By default, data is pulled via httpclient. Use the ResourceType property to read data from a file.
    /// </summary>
    /// <example>
    /// <code>
    /// JsonSource&lt;POCO&gt; source = new JsonSource&lt;POCO&gt;("https://jsonplaceholder.typicode.com/todos");
    /// </code>
    /// </example>
    public class XmlSource<TOutput> : DataFlowStreamSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read Xml from {Uri ?? ""}";

        /// <summary>
        /// The XmlSerializer used to deserialize the xml into the used data type.
        /// </summary>
        public XmlSerializer XmlSerializer { get; set; }

        /// <summary>
        /// The element name of the document that contains an item of the data to be parsed
        /// </summary>

        /* Private stuff */
        XmlReader XmlReader { get; set; }
     
        XmlTypeInfo TypeInfo { get; set; }

        public XmlSource()
        {
            TypeInfo = new XmlTypeInfo(typeof(TOutput));
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
                    if (XmlReader.Name == TypeInfo.ElementName)
                    {
                        XElement el = XNode.ReadFrom(XmlReader) as XElement;
                        if (el != null)
                        {
                            var output = (TOutput)XmlSerializer.Deserialize(el.CreateReader());
                            Buffer.SendAsync(output).Wait();
                            LogProgress();
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
    /// Reads data from a json source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// JsonSource as a nongeneric type returns a dynamic object as output. If you need typed output, use
    /// the JsonSource&lt;TOutput&gt; object instead.
    /// </summary>
    /// <see cref="XmlSource{TOutput}"/>
    /// <example>
    /// <code>
    /// JsonSource source = new JsonSource("demodata.json");
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
