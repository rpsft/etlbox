using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class SqlTaskBulkInsertTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("ControlFlow");
        public SQLiteConnectionManager SQLiteConnection => Config.SQLiteConnection.ConnectionManager("ControlFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> ConnectionsWithValue(int value)
            => Config.AllSqlConnectionsWithValue("ControlFlow", value);


        public SqlTaskBulkInsertTests(ControlFlowDatabaseFixture dbFixture)
        { }


        [Theory, MemberData(nameof(Connections))]

        public void StringArray(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture destTable = new TwoColumnsTableFixture(connection, "BulkInsert2Columns");

            TableData<string[]> data = new TableData<string[]>(destTable.TableDefinition);
            string[] values = { "1", "Test1" };
            data.Rows.Add(values);
            string[] values2 = { "2", "Test2" };
            data.Rows.Add(values2);
            string[] values3 = { "3", "Test3" };
            data.Rows.Add(values3);

            //Act
            SqlTask.BulkInsert(connection, "Bulk insert demo data", data, "BulkInsert2Columns");

            //Assert
            destTable.AssertTestData();
        }

        [Theory, MemberData(nameof(ConnectionsWithValue), 0),
        MemberData(nameof(ConnectionsWithValue), 2),
        MemberData(nameof(ConnectionsWithValue), 3)]
        public void WithIdentityShift(IConnectionManager connection, int identityIndex)
        {
            //SQLite does not support Batch Insert on Non Nullable Identity Columns
            if (connection.GetType() != typeof(SQLiteConnectionManager))
            {
                //Arrange
                FourColumnsTableFixture destTable = new FourColumnsTableFixture(connection, "BulkInsert4Columns", identityIndex);

                TableData data = new TableData(destTable.TableDefinition, 2);
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

    }
}
