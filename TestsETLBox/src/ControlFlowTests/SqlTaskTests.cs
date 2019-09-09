using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class SqlTaskTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("ControlFlow");

        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");
        public static IEnumerable<object[]> ConnectionsWithValue(string value) => Config.AllSqlConnectionsWithValue("ControlFlow", value);


        public SqlTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]
        public void ExecuteNonQuery()
        {
            //Arrange
            string propName = HashHelper.RandomString(10);
            SqlTask.ExecuteNonQuery(SqlConnection,
                "Test add extended property",
                $@"EXEC sp_addextendedproperty @name = N'{propName}', @value = 'Test';");
            //Act
            string actual = SqlTask.ExecuteScalar(SqlConnection,
                "Get reference result",
                $"SELECT value FROM fn_listextendedproperty('{propName}', default, default, default, default, default, default)").ToString();
            //Assert
            Assert.Equal("Test", actual);
        }

        [Fact]
        public void ExecuteNonQueryWithParameter()
        {
            //Arrange
            string propName = HashHelper.RandomString(10);
            var parameter = new List<QueryParameter> { new QueryParameter("propName", "nvarchar(100)", propName) };
            SqlTask.ExecuteNonQuery(SqlConnection,
                "Test add extended property",
                $"EXEC sp_addextendedproperty @name = @propName, @value = 'Test';", parameter);
            //Act
            string actual = SqlTask.ExecuteScalar(SqlConnection,
                "Get reference result",
                $"SELECT value FROM fn_listextendedproperty(@propName, default, default, default, default, default, default)", parameter).ToString();
            //Assert
            Assert.Equal("Test", actual);
        }

        [Fact]
        public void ExecuteScalar()
        {
            //Arrange
            //Act
            object result = SqlTask.ExecuteScalar(SqlConnection,
                "Test execute scalar",
                $@"SELECT CAST('Hallo Welt' AS NVARCHAR(100)) AS ScalarResult");
            //Assert
            Assert.Equal("Hallo Welt", result.ToString());

        }

        [Fact]
        public void ExecuteScalarDatatype()
        {
            //Arrange
            //Act
            decimal result = (decimal)(SqlTask.ExecuteScalar(SqlConnection,
                "Test execute scalar with datatype",
                $@"SELECT CAST(1.343 AS NUMERIC(4,3)) AS ScalarResult"));
            //Assert
            Assert.Equal(1.343m,result);

        }

        [Theory, MemberData(nameof(ConnectionsWithValue),"1"),
        MemberData(nameof(ConnectionsWithValue), "7"),
        MemberData(nameof(ConnectionsWithValue), "NULL"),
        MemberData(nameof(ConnectionsWithValue), "'true'")]
        public void ExecuteScalarAsBool(IConnectionManager connection, string sqlBoolValue)
        {
            //Arrange
            //Act
            bool result = SqlTask.ExecuteScalarAsBool(connection,
                "Test execute scalar as bool",
                $"SELECT {sqlBoolValue} AS Bool");
            //Assert
            if (sqlBoolValue == "NULL")
                Assert.False(result);
            else
                Assert.True(result);
        }

        [Fact]
        public void ExecuteReaderSingleColumn()
        {
            //Arrange
            List<int> asIsResult = new List<int>();
            List<int> toBeResult = new List<int>() { 1, 2, 3 };
            //Act
            SqlTask.ExecuteReader(SqlConnection,
                "Test execute reader",
                "SELECT * FROM (VALUES (1),(2),(3)) MyTable(a)",
                colA => asIsResult.Add((int)colA));
            //Assert
            Assert.Equal(toBeResult, asIsResult);
        }

        [Fact]
        public void ExecuteReaderWithParameter()
        {
            //Arrange
            List<int> asIsResult = new List<int>();
            List<int> toBeResult = new List<int>() { 1 };
            List<QueryParameter> parameter = new List<QueryParameter>() { new QueryParameter("par1", "int", 1) };
            //Act
            SqlTask.ExecuteReader(SqlConnection, "Test execute reader",
                "SELECT * FROM (VALUES (1),(2),(3)) MyTable(a) where a = @par1", parameter,
                colA => asIsResult.Add((int)colA));
            //Assert
            Assert.Equal(toBeResult, asIsResult);
        }

        public class ThreeInteger : IEquatable<ThreeInteger>
        {
            public int A { get; set; }
            public int B { get; set; }
            public int C { get; set; }
            public ThreeInteger() { }
            public ThreeInteger(int a, int b, int c)
            {
                A = a; B = b; C = c;
            }
            public bool Equals(ThreeInteger other) => other != null ? other.A == A && other.B == B && other.C == C : false;
            public override bool Equals(object obj)
            {
                return this.Equals((ThreeInteger)obj);
            }
            public override int GetHashCode()
            {
                return base.GetHashCode();
            }
        }

        [Fact]
        public void ExecuteReaderMultiColumn()
        {
            //Arrange
            List<ThreeInteger> asIsResult = new List<ThreeInteger>();
            List<ThreeInteger> toBeResult = new List<ThreeInteger>() { new ThreeInteger(1, 2, 3), new ThreeInteger(4, 5, 6), new ThreeInteger(7, 8, 9) };
            ThreeInteger CurColumn = new ThreeInteger();
            //Act
            SqlTask.ExecuteReader(SqlConnection,
                "Test execute reader",
                "SELECT * FROM (VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)) AS MyTable(a,b,c)"
                , () => CurColumn = new ThreeInteger()
                , () => asIsResult.Add(CurColumn)
                , colA => CurColumn.A = (int)colA
                , colB => CurColumn.B = (int)colB
                , colC => CurColumn.C = (int)colC
                );
            //Assert
            Assert.Equal(toBeResult, asIsResult);
        }



        [Fact]
        public void BulkInsert()
        {
            //Arrange
            TableDefinition tableDefinition = new TableDefinition("dbo.BulkInsert", new List<TableColumn>() {
                new TableColumn("ID", "int", allowNulls: false,isPrimaryKey:true,isIdentity:true)   ,
                new TableColumn("Col1", "nvarchar(4000)", allowNulls: true),
                new TableColumn("Col2", "nvarchar(4000)", allowNulls: true)
            });
            tableDefinition.CreateTable(SqlConnection);
            TableData data = new TableData(tableDefinition);
            string[] values = { "Value1", "Value2" };
            data.Rows.Add(values);
            string[] values2 = { "Value3", "Value4" };
            data.Rows.Add(values2);
            string[] values3 = { "Value5", "Value6" };
            data.Rows.Add(values3);

            //Act
            SqlTask.BulkInsert(SqlConnection, "Bulk insert demo data", data, "dbo.BulkInsert");

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "dbo.BulkInsert", "Col1 LIKE 'Value%'"));
        }

    }
}
