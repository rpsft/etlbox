using JetBrains.Annotations;

namespace ETLBox.Kafka.Tests
{
    [UsedImplicitly]
    public sealed class KafkaFixture
    {
        public string BootstrapAddress => TestShared.Helper.Config.KafkaBootstrapAddress;
    }
}
