using ETLBox.Kafka.Tests.Utilities;
using JetBrains.Annotations;

namespace ETLBox.Kafka.Tests
{
    [UsedImplicitly]
    public sealed class KafkaContainerFixture : IDisposable
    {
        public string BootstrapAddress => _container.GetBootstrapAddress();

        private readonly KafkaContainer _container;

        public KafkaContainerFixture()
        {
            var builder = new KafkaBuilder();
            _container = builder.Build();
            _container.StartAsync().Wait();
        }

        public void Dispose()
        {
            _container.DisposeAsync();
        }
    }
}
