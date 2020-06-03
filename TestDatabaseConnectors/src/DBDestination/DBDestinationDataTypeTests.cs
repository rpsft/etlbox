using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationDataTypeTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public DbDestinationDataTypeTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyDataTypeRow
        {
            public int IntCol { get; set; }
            public long LongCol { get; set; }
            public decimal DecimalCol { get; set; }
            public double DoubleCol { get; set; }
            public DateTime DateTimeCol { get; set; }
            public DateTime DateCol { get; set; }
            public string StringCol { get; set; }
            public char CharCol { get; set; }
            public string DecimalStringCol { get; set; }
            public string NullCol { get; set; }
            public EnumType EnumCol { get; set; }
        }

        public enum EnumType
        {
            Value1 = 1,
            Value2 = 2
        }

        [Theory, MemberData(nameof(Connections))]
        public void MixedTypes(IConnectionManager connection)
        {
            CreateTestTable(connection, "datatypedestination");
            //Arrange
            MemorySource<MyDataTypeRow> source = new MemorySource<MyDataTypeRow>(
                new List<MyDataTypeRow>() {
                   new MyDataTypeRow() {
                       IntCol = 1,
                       LongCol = -1,
                       DecimalCol = 2.3M,
                       DoubleCol = 5.4,
                       DateTimeCol = new DateTime(2010,1,1,10,10,10),
                       DateCol =  new DateTime(2020,1,1),
                       StringCol = "Test",
                       CharCol = 'T',
                       DecimalStringCol = "13.4566",
                       NullCol = null,
                       EnumCol = EnumType.Value2
                   }
                });

            //Act
            DbDestination<MyDataTypeRow> dest = new DbDestination<MyDataTypeRow>(connection, "datatypedestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            AssertFirstRow(connection, "datatypedestination");
        }

        private static void CreateTestTable(IConnectionManager connection, string tablename)
        {
            DropTableTask.DropIfExists(connection, tablename);
            CreateTableTask.Create(connection, tablename,
                new List<TableColumn>() {
                    new TableColumn("intcol", "INT", allowNulls: true),
                    new TableColumn("LongCol", "BIGINT", allowNulls: true),
                    new TableColumn("DecimalCol", "FLOAT", allowNulls: true),
                    new TableColumn("DoubleCol", "FLOAT", allowNulls: true),
                    new TableColumn("DateTimeCol", "DATETIME", allowNulls: true),
                    new TableColumn("DateCol", "DATE", allowNulls: true),
                    new TableColumn("StringCol", "VARCHAR(200)", allowNulls: true),
                    new TableColumn("CharCol", "CHAR(1)", allowNulls: true),
                    new TableColumn("DecimalStringCol", "DECIMAL(12,10)", allowNulls: true),
                    new TableColumn("NullCol", "CHAR(1)", allowNulls: true),
                    new TableColumn("EnumCol", "INT", allowNulls: true),
                });
        }

        private static void AssertFirstRow(IConnectionManager connection, string tableName)
        {
            //            IntCol LongCol DecimalCol DoubleCol   DateTimeCol DateCol StringCol CharCol DecimalStringCol NullCol
            //1 - 1  2.3 5.4 2010 - 01 - 01 10:10:10.100 2020 - 01 - 01  Test T   13.4566000000   NULL
            SqlTask.ExecuteReaderSingleLine(connection, "Check data", $"SELECT * FROM {tableName} WHERE intcol = 1",
                col => Assert.True(Convert.ToInt32(col) == 1),
                col => Assert.True(Convert.ToInt64(col) == -1),
                col => Assert.True(Convert.ToDecimal(col) == 2.3M),
                col => Assert.True(Convert.ToDecimal(col) == 5.4M),
                col => Assert.True(Convert.ToDateTime(col) == new DateTime(2010, 1, 1, 10, 10, 10)),
                col => Assert.True(Convert.ToDateTime(col) == new DateTime(2020, 1, 1)),
                col => Assert.True(Convert.ToString(col) == "Test"),
                col => Assert.True(Convert.ToString(col) == "T" || Convert.ToString(col) == "84"),
                col => Assert.True(Convert.ToString(col).Replace("0", "") == "13.4566"),
                col => Assert.True(col == null),
                col => Assert.True(Convert.ToInt32(col) == 2)
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void MixedTypesWithDynamic(IConnectionManager connection)
        {
            CreateTestTable(connection, "datatypedestinationdynamic");
            //Arrange
            MemorySource source = new MemorySource();
            dynamic d1 = new ExpandoObject();
            d1.IntCol = 1;
            d1.LongCol = -1;
            d1.DecimalCol = 2.3M;
            d1.DoubleCol = 5.4;
            d1.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d1.DateCol = new DateTime(2020, 1, 1);
            d1.StringCol = "Test";
            d1.CharCol = 'T';
            d1.DecimalStringCol = "13.4566";
            d1.NullCol = null;
            d1.EnumCol = EnumType.Value2;

            //Act
            DbDestination dest = new DbDestination(connection, "datatypedestinationdynamic");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            AssertFirstRow(connection, "datatypedestinationdynamic");
        }



        [Theory, MemberData(nameof(Connections))]
        public void MixedOrderOfProps(IConnectionManager connection)
        {
            CreateTestTable(connection, "datatypedestinationdynamicmixed");
            //Arrange
            MemorySource source = new MemorySource();
            dynamic d1 = new ExpandoObject();
            d1.IntCol = 1;
            d1.LongCol = -1;
            d1.DecimalCol = 2.3M;
            d1.DoubleCol = 5.4;
            d1.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d1.DateCol = new DateTime(2020, 1, 1);
            d1.StringCol = "Test";
            d1.CharCol = 'T';
            d1.DecimalStringCol = "13.4566";
            d1.NullCol = null;
            d1.EnumCol = EnumType.Value2;
            source.DataAsList.Add(d1);

            dynamic d2 = new ExpandoObject();
            d2.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d2.IntCol = 2;
            d2.LongCol = -1;
            d2.DoubleCol = 5.4;
            d2.DateCol = new DateTime(2020, 1, 1);
            source.DataAsList.Add(d2);

            //Act
            DbDestination dest = new DbDestination(connection, "datatypedestinationdynamicmixed");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            AssertFirstRow(connection, "datatypedestinationdynamicmixed");

        }


        [Theory, MemberData(nameof(Connections))]
        public void NullValuesFirst(IConnectionManager connection)
        {
            CreateTestTable(connection, "datatypedestinationdynamicmixed");
            //Arrange
            MemorySource source = new MemorySource();

            dynamic d1 = new ExpandoObject();
            d1.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d1.IntCol = 2;
            d1.LongCol = -1;
            d1.DoubleCol = 5.4;
            d1.DateCol = new DateTime(2020, 1, 1);
            source.DataAsList.Add(d1);

            dynamic d2 = new ExpandoObject();
            d2.IntCol = 1;
            d2.LongCol = -1;
            d2.DecimalCol = 2.3M;
            d2.DoubleCol = 5.4;
            d2.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d2.DateCol = new DateTime(2020, 1, 1);
            d2.StringCol = "Test";
            d2.CharCol = 'T';
            d2.DecimalStringCol = "13.4566";
            d2.NullCol = null;
            d2.EnumCol = EnumType.Value2;
            source.DataAsList.Add(d2);

            //Act
            DbDestination dest = new DbDestination(connection, "datatypedestinationdynamicmixed");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            AssertFirstRow(connection, "datatypedestinationdynamicmixed");

        }

        [Theory, MemberData(nameof(Connections))]
        public void NullValuesFirstAcrossBatches(IConnectionManager connection)
        {
            CreateTestTable(connection, "datatypedestinationdynamicmixed");
            //Arrange
            MemorySource source = new MemorySource();

            dynamic d1 = new ExpandoObject();
            d1.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d1.IntCol = 2;
            d1.LongCol = -1;
            d1.DoubleCol = 5.4;
            d1.DateCol = new DateTime(2020, 1, 1);
            source.DataAsList.Add(d1);

            dynamic d2 = new ExpandoObject();
            d2.CharCol = 'X';
            d2.DecimalStringCol = "15";
            d2.NullCol = null;
            d2.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d2.IntCol = 2;
            source.DataAsList.Add(d2);

            dynamic d3 = new ExpandoObject();
            d3.StringCol = "Test";
            source.DataAsList.Add(d3);

            dynamic d4 = new ExpandoObject();
            d4.IntCol = 1;
            d4.LongCol = -1;
            d4.DecimalCol = 2.3M;
            d4.DoubleCol = 5.4;
            d4.DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10);
            d4.DateCol = new DateTime(2020, 1, 1);
            d4.StringCol = "Test";
            d4.CharCol = 'T';
            d4.DecimalStringCol = "13.4566";
            d4.NullCol = null;
            d4.EnumCol = EnumType.Value2;
            source.DataAsList.Add(d4);

            //Act
            DbDestination dest = new DbDestination(connection, "datatypedestinationdynamicmixed", batchSize: 2);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            AssertFirstRow(connection, "datatypedestinationdynamicmixed");

        }



    }
}
