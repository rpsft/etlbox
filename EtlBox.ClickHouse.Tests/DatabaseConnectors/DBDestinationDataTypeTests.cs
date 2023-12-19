using System.Globalization;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DatabaseConnectors
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbDestinationDataTypeTests : DatabaseTestBase
    {
        private readonly IConnectionManager connection;

        protected DbDestinationDataTypeTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Theory]
        [InlineData("ru-RU")]
        [InlineData("en-US")]
        public void MixedTypes(string language)
        {
            var culture = CultureInfo.GetCultureInfo(language);
            var previousCulture = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = culture;

                CreateTableTask.Create(
                    connection,
                    "datatypedestination",
                    new List<TableColumn>
                    {
                        new("IntCol", "INT", allowNulls: true),
                        new("LongCol", "BIGINT", allowNulls: true),
                        new("DecimalCol", "FLOAT", allowNulls: true),
                        new("DoubleCol", "FLOAT", allowNulls: true),
                        new("DateTimeCol", "DATETIME", allowNulls: true),
                        new("DateCol", "DATE", allowNulls: true),
                        new("StringCol", "VARCHAR(200)", allowNulls: true),
                        new("CharCol", "CHAR(1)", allowNulls: true),
                        new("DecimalStringCol", "DECIMAL(12,10)", allowNulls: true),
                        new("NullCol", "CHAR(1)", allowNulls: true),
                        new("EnumCol", "INT", allowNulls: true)
                    }
                );
                //Arrange
                var connectionCulture = connection.ConnectionCulture;

                var source = new MemorySource<MyDataTypeRow>(
                    new List<MyDataTypeRow>
                    {
                        new()
                        {
                            IntCol = 1,
                            LongCol = -1,
                            DecimalCol = 2.3M,
                            DoubleCol = 5.4,
                            DateTimeCol = new DateTime(2010, 1, 1, 10, 10, 10),
                            DateCol = new DateTime(2020, 1, 1),
                            StringCol = "Test",
                            CharCol = 'T',
                            DecimalStringCol = 13.4566m.ToString(connectionCulture),
                            NullCol = null,
                            EnumCol = EnumType.Value2
                        }
                    }
                );

                //Act
                var dest = new DbDestination<MyDataTypeRow>(connection, "datatypedestination");
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();

                //Assert
                //            IntCol LongCol DecimalCol DoubleCol   DateTimeCol DateCol StringCol CharCol DecimalStringCol NullCol
                //1 - 1  2.3 5.4 2010 - 01 - 01 10:10:10.100 2020 - 01 - 01  Test T   13.4566000000   NULL
                SqlTask.ExecuteReaderSingleLine(
                    connection,
                    "Check data",
                    "SELECT * FROM datatypedestination",
                    col => Assert.True(Convert.ToInt32(col) == 1),
                    col => Assert.True(Convert.ToInt64(col) == -1),
                    col => Assert.True(Convert.ToDecimal(col) == 2.3M),
                    col => Assert.True(Convert.ToDecimal(col) == 5.4M),
                    col =>
                        Assert.True(
                            Convert.ToDateTime(col) == new DateTime(2010, 1, 1, 10, 10, 10)
                        ),
                    col => Assert.True(Convert.ToDateTime(col) == new DateTime(2020, 1, 1)),
                    col => Assert.True(Convert.ToString(col) == "Test"),
                    col =>
                        Assert.True(Convert.ToString(col) == "T" || Convert.ToString(col) == "84"),
                    col =>
                        Assert.True(
                            Convert.ToString(col, connectionCulture)?.Replace("0", "")
                                == 13.4566m.ToString(connectionCulture)
                        ),
                    col => Assert.True(col == null),
                    col => Assert.True(Convert.ToInt32(col) == 2)
                );
            }
            finally
            {
                CultureInfo.CurrentCulture = previousCulture;
            }
        }

        public class MyDataTypeRow
        {
            public int IntCol { get; set; }
            public long LongCol { get; set; }
            public decimal DecimalCol { get; set; }
            public double DoubleCol { get; set; }
            public DateTime DateTimeCol { get; set; }
            public DateTime DateCol { get; set; }
            public string? StringCol { get; set; }
            public char CharCol { get; set; }
            public string? DecimalStringCol { get; set; }
            public string? NullCol { get; set; }
            public EnumType EnumCol { get; set; }
        }

        public enum EnumType
        {
            Value1 = 1,
            Value2 = 2
        }

        public class SqlServer : DbDestinationDataTypeTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationDataTypeTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
