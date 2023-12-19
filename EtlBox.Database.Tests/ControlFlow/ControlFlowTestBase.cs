using EtlBox.Database.Tests.Infrastructure;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ControlFlow
{
    public abstract class ControlFlowTestBase: DatabaseTestBase
    {
        protected ControlFlowTestBase(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
        }

        protected IConnectionManager ConnectionManager =>
            _fixture.GetContainer(_connectionType).GetConnectionManager();
    }
}
