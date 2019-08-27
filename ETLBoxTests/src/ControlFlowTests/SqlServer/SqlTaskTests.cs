using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("Sql Server ControlFlow")]
    public class SqlTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public SqlTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]
        public void ExecuteNonQuery()
        {
            //Arrange
            string propName = HashHelper.RandomString(10);
            SqlTask.ExecuteNonQuery(Connection,
                "Test add extended property",
                $@"EXEC sp_addextendedproperty @name = N'{propName}', @value = 'Test';");
            //Act
            string actual = SqlTask.ExecuteScalar(Connection,
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
            SqlTask.ExecuteNonQuery(Connection,
                "Test add extended property",
                $"EXEC sp_addextendedproperty @name = @propName, @value = 'Test';", parameter);
            //Act
            string actual = SqlTask.ExecuteScalar(Connection,
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
            object result = SqlTask.ExecuteScalar(Connection,
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
            decimal result = (decimal)(SqlTask.ExecuteScalar(Connection,
                "Test execute scalar with datatype",
                $@"SELECT CAST(1.343 AS NUMERIC(4,3)) AS ScalarResult"));
            //Assert
            Assert.Equal(1.343m,result);

        }

        [Fact]
        public void ExecuteScalarAsBool()
        {
            //Arrange
            //Act
            bool result = SqlTask.ExecuteScalarAsBool(Connection,
                "Test execute scalar as bool",
                "SELECT 1 AS Bool");
            //Assert
            Assert.True(result);
        }

        [Fact]
        public void ExecuteReaderSingleColumn()
        {
            //Arrange
            List<int> asIsResult = new List<int>();
            List<int> toBeResult = new List<int>() { 1, 2, 3 };
            //Act
            SqlTask.ExecuteReader(Connection,
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
            SqlTask.ExecuteReader(Connection, "Test execute reader",
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
            SqlTask.ExecuteReader(Connection,
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
            tableDefinition.CreateTable(Connection);
            TableData data = new TableData(tableDefinition);
            string[] values = { "Value1", "Value2" };
            data.Rows.Add(values);
            string[] values2 = { "Value3", "Value4" };
            data.Rows.Add(values2);
            string[] values3 = { "Value5", "Value6" };
            data.Rows.Add(values3);

            //Act
            SqlTask.BulkInsert(Connection, "Bulk insert demo data", data, "dbo.BulkInsert");

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "dbo.BulkInsert", "Col1 LIKE 'Value%'"));
        }

    }
}
