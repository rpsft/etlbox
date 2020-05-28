using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class DropSchemaTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllConnectionsWithoutSQLite("ControlFlow");

        public DropSchemaTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void Drop(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager))
            {
                //Arrange
                CreateSchemaTask.Create(connection, "testcreateschema");
                Assert.True(IfSchemaExistsTask.IsExisting(connection, "testcreateschema"));

                //Act
                DropSchemaTask.Drop(connection, "testcreateschema");

                //Assert
                Assert.False(IfSchemaExistsTask.IsExisting(connection, "testcreateschema"));
            }
        }

        [Theory, MemberData(nameof(Connections))]
        public void DropIfExists(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(MySqlConnectionManager))
            {
                //Arrange
                DropSchemaTask.DropIfExists(connection, "testcreateschema2");
                CreateSchemaTask.Create(connection, "testcreateschema2");
                Assert.True(IfSchemaExistsTask.IsExisting(connection, "testcreateschema2"));

                //Act
                DropSchemaTask.DropIfExists(connection, "testcreateschema2");

                //Assert
                Assert.False(IfSchemaExistsTask.IsExisting(connection, "testcreateschema2"));
            }
        }


        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropSchemaTask.Drop(Config.SQLiteConnection.ConnectionManager("ControlFlow"), "Test")
                );
        }

        [Fact]
        public void NotSupportedWithMySql()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropSchemaTask.Drop(Config.MySqlConnection.ConnectionManager("ControlFlow"), "Test")
                );
        }
    }
}
