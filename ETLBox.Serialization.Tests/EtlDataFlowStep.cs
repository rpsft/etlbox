using System.Collections.Concurrent;
using System.Dynamic;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using ALE.ETLBox.Serialization;
using ALE.ETLBox.Serialization.DataFlow;
using ETLBox.Primitives;

namespace ETLBox.Serialization.Tests
{
    [Serializable]
    public class EtlDataFlowStep : IDataFlow, IXmlSerializable
    {
        private readonly ConcurrentDictionary<
            (Type type, string? key),
            IConnectionManager
        > _connectionManagers = new();

        public Guid? ReferenceId { get; set; }

        public string? Name { get; set; }

        public int? TimeoutMilliseconds { get; set; }

        public IConnectionManager GetOrAddConnectionManager(
            Type connectionManagerType,
            string? key,
            Func<Type, string?, IConnectionManager> factory
        ) =>
            _connectionManagers.GetOrAdd((connectionManagerType, key), k => factory(k.type, k.key));

        public IDataFlowSource<ExpandoObject> Source { get; set; } = null!;

        public IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; } = null!;

        public IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; } = null!;

        public XmlSchema? GetSchema() => null;

        public virtual void ReadXml(XmlReader reader)
        {
            var xmlReader = new DataFlowXmlReader(this);
            xmlReader.Read(reader);
        }

        public void WriteXml(XmlWriter writer)
        {
            throw new NotSupportedException();
        }

        public void Invoke(CancellationToken cancellationToken)
        {
            Source.Execute(cancellationToken);
            var tasks = Destinations
                .Select(d => d.Completion)
                .Concat(ErrorDestinations.Select(ed => ed.Completion))
                .ToArray();
            Task.WaitAll(tasks, CancellationToken.None);
        }

        /// <summary>
        /// Method for check a connectionManagers added for dispose
        /// </summary>
        /// <returns></returns>
        public int ConnectionManagerCount() => _connectionManagers.Count;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this); // Violates rule
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            foreach (var value in _connectionManagers.Values)
            {
                value.Dispose();
            }
        }
    }
}
