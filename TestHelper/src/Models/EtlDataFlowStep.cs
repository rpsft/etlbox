using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using ALE.ETLBox.Helper.DataFlow;
using ETLBox.Primitives;
using Microsoft.Extensions.Logging;

namespace TestHelper.Models
{
    [Serializable]
    public class EtlDataFlowStep : IDataFlow, IXmlSerializable
    {
        private readonly ILogger _logger;

        public EtlDataFlowStep(ILogger logger) 
        {
            _logger = logger;
        }

        public Guid? ReferenceId { get; set; }

        public string Name { get; set; }

        public int? TimeoutMilliseconds { get; set; }

        public IDataFlowSource<ExpandoObject> Source { get; set; }

        public IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; }

        public IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; }

        public XmlSchema GetSchema() => null;

        public virtual void ReadXml(XmlReader reader)
        {
            this.ReadFromXml(reader, _logger);
        }

        public void WriteXml(XmlWriter writer)
        {
            throw new NotImplementedException();
        }

        public void Invoke(CancellationToken cancellationToken)
        {
            Source.Execute(cancellationToken);
            var tasks = Destinations.Select(d => d.Completion)
                .Concat(ErrorDestinations.Select(ed => ed.Completion))
                .ToArray();
            Task.WaitAll(tasks);
        }
    }
}
