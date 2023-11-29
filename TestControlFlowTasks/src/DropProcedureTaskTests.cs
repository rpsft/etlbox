using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    public class DropProcedureTaskTests : ControlFlowTestBase
    {
        public DropProcedureTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLite;

        [Theory, MemberData(nameof(Connections))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            CreateProcedureTask.CreateOrAlter(connection, "DropProc1", "SELECT 1;");
            Assert.True(IfProcedureExistsTask.IsExisting(connection, "DropProc1"));

            //Act
            DropProcedureTask.Drop(connection, "DropProc1");

            //Assert
            Assert.False(IfProcedureExistsTask.IsExisting(connection, "DropProc1"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void DropIfExists(IConnectionManager connection)
        {
            //Arrange
            DropProcedureTask.DropIfExists(connection, "DropProc2");
            CreateProcedureTask.CreateOrAlter(connection, "DropProc2", "SELECT 1;");
            Assert.True(IfProcedureExistsTask.IsExisting(connection, "DropProc2"));

            //Act
            DropProcedureTask.DropIfExists(connection, "DropProc2");

            //Assert
            Assert.False(IfProcedureExistsTask.IsExisting(connection, "DropProc2"));
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropProcedureTask.Drop(SqliteConnection, "Test")
            );
        }
    }
}
