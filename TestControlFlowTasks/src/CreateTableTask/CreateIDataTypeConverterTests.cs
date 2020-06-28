using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Exceptions;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateIDataTypeConverterTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("ControlFlow");
        public CreateIDataTypeConverterTests(ControlFlowDatabaseFixture dbFixture)
        { }

        public class MyDataTypeConverter : IDataTypeConverter
        {
            public string TryConvertDbDataType(string dbSpecificTypeName, ConnectionManagerType connectionType)
            {
                if (dbSpecificTypeName == "ABC")
                    return "DATETIME";
                else
                    return DataTypeConverter.TryGetDbSpecificType(dbSpecificTypeName, connectionType);
            }
        }

        [Fact]
        public void WithOwnImplementation()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("somedate", "ABC"),
                new TableColumn("sometext", "TEXT")
            };
            //Act
            var ctt = new CreateTableTask("CreateTableIDataTypeConverter", columns)
            {
                ConnectionManager = SqlConnection,
                DataTypeConverter = new MyDataTypeConverter()
            };
            ctt.Create();
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(SqlConnection, "CreateTableIDataTypeConverter"));
            var td = TableDefinition.FromTableName(SqlConnection, "CreateTableIDataTypeConverter");

            Assert.Collection<TableColumn>(td.Columns,
                col => Assert.True(col.DataType == "DATETIME"),
                col => Assert.True(col.DataType == "VARCHAR(MAX)")
                );
        }
    }
}
