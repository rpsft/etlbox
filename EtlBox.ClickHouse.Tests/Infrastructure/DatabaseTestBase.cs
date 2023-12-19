using ALE.ETLBox.ConnectionManager;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.Infrastructure
{
    public abstract class DatabaseTestBase
    {
        protected readonly ConnectionManagerType _connectionType;
        protected readonly ITestOutputHelper _logger;
        protected readonly DatabaseFixture _fixture;

        protected DatabaseTestBase(DatabaseFixture fixture, ConnectionManagerType connectionType, ITestOutputHelper logger)
        {
            _connectionType = connectionType;
            _logger = logger;
            _fixture = fixture;
        }

        public string QB => _fixture.QB(_connectionType);

        public string QE => _fixture.QE(_connectionType);
    }
}
