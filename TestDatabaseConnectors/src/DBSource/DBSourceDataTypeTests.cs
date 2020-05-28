using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbSourceDataTypeTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DbSourceDataTypeTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyDataTypeRow
        {
            [ColumnMap("int_col")]
            public int IntCol { get; set; }
            [ColumnMap("long_col")]
            public long LongCol { get; set; }
            [ColumnMap("decimal_col")]
            public decimal DecimalCol { get; set; }
            [ColumnMap("double_col")]
            public double DoubleCol { get; set; }
            [ColumnMap("datetime_col")]
            public DateTime DateTimeCol { get; set; }
            [ColumnMap("date_col")]
            public DateTime DateCol { get; set; }
            [ColumnMap("string_col")]
            public string StringCol { get; set; }
            [ColumnMap("char_col")]
            public char CharCol { get; set; }
            [ColumnMap("decimal_string_col")]
            public string DecimalStringCol { get; set; }
            [ColumnMap("null_col")]
            public string NullCol { get; set; }
            [ColumnMap("enum_col")]
            public EnumType EnumCol { get; set; }
        }


        public enum EnumType
        {
            Value1 = 1,
            Value2 = 2
        }

        [Theory, MemberData(nameof(Connections))]
        public void ReadDifferentTypes(IConnectionManager connection)
        {
            //Arrange
            CreateTableTask.Create(connection, "different_type_table",
                new List<TableColumn>() {
                    new TableColumn("int_col", "INT", allowNulls: true),
                    new TableColumn("long_col", "BIGINT", allowNulls: true),
                    new TableColumn("decimal_col", "FLOAT", allowNulls: true),
                    new TableColumn("double_col", "FLOAT", allowNulls: true),
                    new TableColumn("datetime_col", "DATETIME", allowNulls: true),
                    new TableColumn("date_col", "DATE", allowNulls: true),
                    new TableColumn("string_col", "VARCHAR(200)", allowNulls: true),
                    new TableColumn("char_col", "CHAR(1)", allowNulls: true),
                    new TableColumn("decimal_string_col", "DECIMAL(12,10)", allowNulls: true),
                    new TableColumn("null_col", "CHAR(1)", allowNulls: true),
                    new TableColumn("enum_col", "INT", allowNulls: true),
                });

            SqlTask.ExecuteNonQuery(connection, "Insert test data",
                @"INSERT INTO different_type_table 
                    (int_col, long_col, decimal_col, double_col, datetime_col, date_col
, string_col, char_col, decimal_string_col, null_col, enum_col) 
                VALUES (1, -1, 2.3, 5.4, '2010-01-01 10:00:00.000', '2020-01-01', 'Test', 'T', '13.4566', NULL, 2 )");
            //Act
            DbSource<MyDataTypeRow> source = new DbSource<MyDataTypeRow>(connection, "different_type_table");
            MemoryDestination<MyDataTypeRow> dest = new MemoryDestination<MyDataTypeRow>();

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(1, dest.Data.First().IntCol);
            Assert.Equal(-1, dest.Data.First().LongCol);
            Assert.Equal(2.3M, dest.Data.First().DecimalCol);
            Assert.True(dest.Data.First().DoubleCol >= 5.4 && dest.Data.First().DoubleCol < 5.5);
            Assert.Equal("2010-01-01 10:00:00.000", dest.Data.First().DateTimeCol.ToString("yyyy-MM-dd hh:mm:ss.fff"));
            Assert.Equal("2020-01-01", dest.Data.First().DateCol.ToString("yyyy-MM-dd"));
            Assert.Equal("Test", dest.Data.First().StringCol);
            Assert.Equal('T', dest.Data.First().CharCol);
            Assert.StartsWith("13.4566", dest.Data.First().DecimalStringCol);
            Assert.Null(dest.Data.First().NullCol);
            Assert.Equal(EnumType.Value2, dest.Data.First().EnumCol);
        }
    }
}
