using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;
using TestShared.SharedFixtures;

namespace TestControlFlowTasks;

[Collection("ControlFlow")]
public class SqlTaskBulkInsertTests : ControlFlowTestBase
{
    public SqlTaskBulkInsertTests(ControlFlowDatabaseFixture fixture)
        : base(fixture) { }

    public static IEnumerable<object[]> Connections =>
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? AllSqlConnections.Concat(AccessConnection)
            : AllSqlConnections;

    public static IEnumerable<object[]> ConnectionsWithValue(int value)
    {
        return new[]
        {
            new object[] { SqlConnection, value },
            new object[] { PostgresConnection, value },
            new object[] { MySqlConnection, value },
            new object[] { ClickHouseConnection, value }
        };
    }

    [Theory]
    [MemberData(nameof(Connections))]
    //If access fails with "Internal OLE Automation error", download and install: https://www.microsoft.com/en-us/download/confirmation.aspx?id=50040
    //see also: https://stackoverflow.com/questions/54632928/internal-ole-automation-error-in-ms-access-using-oledb
    public void StringArray(IConnectionManager connection)
    {
        //Arrange
        var destTable = new TwoColumnsTableFixture(connection, "BulkInsert2Columns");

        var data = new TableData<string[]>(destTable.TableDefinition);
        object[] values = { "1", "Test1" };
        data.Rows.Add(values);
        object[] values2 = { "2", "Test2" };
        data.Rows.Add(values2);
        object[] values3 = { "3", "Test3" };
        data.Rows.Add(values3);

        //Act
        SqlTask.BulkInsert(connection, "Bulk insert demo data", data, "BulkInsert2Columns");

        //Assert
        destTable.AssertTestData();

        if (connection.GetType() == typeof(AccessOdbcConnectionManager))
            connection.Close();
        //Assert connection is closed
        Assert.True(connection.State == null);
    }

    [Theory]
    [MemberData(nameof(ConnectionsWithValue), 0)]
    [MemberData(nameof(ConnectionsWithValue), 2)]
    [MemberData(nameof(ConnectionsWithValue), 3)]
    public void WithIdentityShift(IConnectionManager connection, int identityIndex)
    {
        //Arrange
        var destTable = new FourColumnsTableFixture(
            connection,
            "BulkInsert4Columns",
            identityIndex
        );

        var data = new TableData(destTable.TableDefinition, 2);
        object[] values = { "Test1", null, 1.2 };
        data.Rows.Add(values);
        object[] values2 = { "Test2", 4711, 1.23 };
        data.Rows.Add(values2);
        object[] values3 = { "Test3", 185, 1.234 };
        data.Rows.Add(values3);

        //Act
        SqlTask.BulkInsert(connection, "Bulk insert demo data", data, "BulkInsert4Columns");

        //Assert
        destTable.AssertTestData();
    }
}
