using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks.Postgres
{
    public class TableDefinitionTests : ControlFlowTestBase
    {
        public TableDefinitionTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void Identity()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(
                PostgresConnection,
                "Create table",
                @"
CREATE TABLE identity (
    document_id serial PRIMARY KEY,
     header_text VARCHAR (255) NOT NULL
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(PostgresConnection, "identity");

            //Assert
            Assert.Collection(
                result.Columns,
                tc =>
                    Assert.True(
                        tc.DataType == "integer"
                            && tc.NETDataType == typeof(int)
                            && tc.IsIdentity
                            && tc.IsPrimaryKey
                    ),
                tc => AssertTypes(tc, "varchar(255)", typeof(string))
            );
        }

        [Fact]
        public void DateTypes()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(
                PostgresConnection,
                "Create table",
                @"
CREATE TABLE datetimetypes (
    datetype DATE,
    timetype TIME,
    timetypewithtimezone TIMETZ,
    intervaltype INTERVAL,
    tswithouttimezone TIMESTAMP,
    tswithtimezone TIMESTAMPTZ
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(
                PostgresConnection,
                "datetimetypes"
            );

            //Assert
            Assert.Collection(
                result.Columns,
                tc => AssertTypes(tc, "date", typeof(DateTime)),
                tc =>
                    Assert.True(
                        tc.DataType == "time"
                            && tc.NETDataType == typeof(DateTime)
                            && tc.NETDateTimeKind == DateTimeKind.Unspecified
                    ),
                tc =>
                    Assert.True(
                        tc.DataType == "timetz"
                            && tc.NETDataType == typeof(DateTime)
                            && tc.NETDateTimeKind == DateTimeKind.Utc
                    ),
                tc =>
                    Assert.True(
                        tc.DataType == "interval"
                            && tc.NETDataType == typeof(string)
                            && tc.NETDateTimeKind == null
                    ),
                tc =>
                    Assert.True(
                        tc.DataType == "timestamp"
                            && tc.NETDataType == typeof(DateTime)
                            && tc.NETDateTimeKind == DateTimeKind.Unspecified
                    ),
                tc =>
                    Assert.True(
                        tc.DataType == "timestamptz"
                            && tc.NETDataType == typeof(DateTime)
                            && tc.NETDateTimeKind == DateTimeKind.Utc
                    )
            );
        }

        [Fact]
        public void Character_Varying()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(
                PostgresConnection,
                "Create table",
                @"
CREATE TABLE varchartable (
    varchar_50 VARCHAR (50) NULL,
    varchar_novalue VARCHAR NULL,
    varchar_charvary CHARACTER VARYING (50),
    varchar_character CHARACTER (10),
    varchar_char CHAR,
    varchar_text TEXT
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(
                PostgresConnection,
                "varchartable"
            );

            //Assert
            Assert.Collection(
                result.Columns,
                tc => AssertTypes(tc, "varchar(50)", typeof(string)),
                tc => AssertTypes(tc, "varchar", typeof(string)),
                tc => AssertTypes(tc, "varchar(50)", typeof(string)),
                tc => AssertTypes(tc, "char(10)", typeof(string)),
                tc => AssertTypes(tc, "char(1)", typeof(string)),
                tc => AssertTypes(tc, "text", typeof(string))
            );
        }

        [Fact]
        public void Numeric()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(
                PostgresConnection,
                "Create table",
                @"
CREATE TABLE numerictable (
    si SMALLINT NULL,
    i INT NULL,
    bi BIGINT,
    dec DECIMAL(10,5),
    dec_2 DECIMAL(10),
    dec_3 DECIMAL,
    num NUMERIC (1,1),
    num_2 NUMERIC (2),
    num_3 NUMERIC,
    r REAL,
    d DOUBLE PRECISION
)"
            );

            //Act
            var result = TableDefinition.GetDefinitionFromTableName(
                PostgresConnection,
                "numerictable"
            );

            //Assert
            Assert.Collection(
                result.Columns,
                tc => AssertTypes(tc, "smallint", typeof(short)),
                tc => AssertTypes(tc, "integer", typeof(int)),
                tc => AssertTypes(tc, "bigint", typeof(long)),
                tc => AssertTypes(tc, "numeric(10,5)", typeof(decimal)),
                tc => AssertTypes(tc, "numeric(10,0)", typeof(decimal)),
                tc => AssertTypes(tc, "numeric", typeof(decimal)),
                tc => AssertTypes(tc, "numeric(1,1)", typeof(decimal)),
                tc => AssertTypes(tc, "numeric(2,0)", typeof(decimal)),
                tc => AssertTypes(tc, "numeric", typeof(decimal)),
                tc => AssertTypes(tc, "real", typeof(double)),
                tc => AssertTypes(tc, "double precision", typeof(double))
            );
        }

        private static void AssertTypes(TableColumn tc, string dataType, Type dotnetType)
        {
            Assert.Multiple(
                () => Assert.Equal(tc.DataType, dataType),
                () => Assert.Equal(tc.NETDataType, dotnetType)
            );
        }
    }
}