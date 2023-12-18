using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper.DataFlow;
using ALE.ETLBox.src.Definitions.DataFlow;

namespace TestHelper.Models
{
    [Serializable]
    public class EtlDataFlowStep : IDataFlow, IXmlSerializable
    {
        public Guid? ReferenceId { get; set; }

        public string Name { get; set; }

        public int? TimeoutMilliseconds { get; set; }

        public IDataFlowSource<ExpandoObject> Source { get; set; }

        public IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; }

        public IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; }

        public XmlSchema GetSchema() => null;

        public virtual void ReadXml(XmlReader reader)
        {
            this.ReadFromXml(reader);
        }

        public void WriteXml(XmlWriter writer)
        {
            throw new NotImplementedException();
        }

        public void Invoke()
        {
            Source.Execute();
            var tasks = Destinations.Select(d => d.Completion)
                .Concat(ErrorDestinations.Select(ed => ed.Completion))
                .ToArray();
            Task.WaitAll(tasks);
        }
    }
}
