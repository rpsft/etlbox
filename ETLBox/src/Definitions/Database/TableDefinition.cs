using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ALE.ETLBox
{
    public class TableDefinition
    {
        public string Name { get; set; }
        public List<TableColumn> Columns { get; set; }
        public int? IDColumnIndex
        {
            get
            {
                TableColumn idCol = Columns.FirstOrDefault(col => col.IsIdentity);
                if (idCol != null)
                    return Columns.IndexOf(idCol);
                else
                    return null;
            }
        }

        public string AllColumnsWithoutIdentity => Columns.Where(col => !col.IsIdentity).AsString();


        public TableDefinition()
        {
            Columns = new List<TableColumn>();
        }

        public TableDefinition(string name) : this()
        {
            Name = name;
        }

        public TableDefinition(string name, List<TableColumn> columns) : this(name)
        {
            Columns = columns;
        }

        public void CreateTable() => CreateTableTask.Create(this);

        public void CreateTable(IConnectionManager connectionManager) => CreateTableTask.Create(connectionManager, this);

        public static TableDefinition GetDefinitionFromTableName(string tableName, IConnectionManager connection)
        {
            IfExistsTask.ThrowExceptionIfNotExists(connection, tableName);
            ConnectionManagerType connectionType = ConnectionManagerTypeFinder.GetType(connection);

            //return ReadTableDefinitionFromDataTable(tableName, connection);
            if (connectionType == ConnectionManagerType.SqlServer)
            {
                return ReadTableDefinitionFromSqlServer(tableName, connection);
            }
            else if (connectionType == ConnectionManagerType.SQLLite)
            {
                return ReadTableDefinitionFromSQLite(tableName, connection);
            }
            else
            {
                throw new ETLBoxException("Unknown connection type - please pass a valid TableDefinition!");
            }
        }

        //private static TableDefinition ReadTableDefinitionFromDataTable(string tableName, IConnectionManager connection)
        //{
        //    connection.Open();
        //    var command = connection.CreateCommand($"SELECT * FROM {tableName} WHERE 1=2", null);
        //    var reader = command.ExecuteReader(CommandBehavior.SingleRow);
        //    DataTable dt = new DataTable();
        //    dt.Load(reader);
        //    connection.Close();
        //    return null;

        //}

        private static TableDefinition ReadTableDefinitionFromSqlServer(string tableName, IConnectionManager connection)
        {
            TableDefinition result = new TableDefinition(tableName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {tableName}",
$@" 
SELECT  cols.name
     , UPPER(tpes.name) AS type_name
     , cols.is_nullable
     , cols.is_identity
     , ident.seed_value
     , ident.increment_value   
     , CONVERT (BIT, CASE WHEN pkconstr.type IS NULL THEN 0 ELSE 1 END ) AS primary_key
     , defconstr.definition AS default_value
    , defconstr.name AS default_constraint_name
     , cols.collation_name
     , compCol.definition AS computed_column_definition
FROM sys.columns cols
INNER JOIN sys.tables tbl
  ON cols.object_id = tbl.object_id
INNER JOIN sys.schemas sc
  ON tbl.schema_id = sc.schema_id
INNER JOIN sys.systypes tpes
  ON tpes.xtype = cols.system_type_id
LEFT JOIN sys.identity_columns ident
  ON ident.object_id = cols.object_id
LEFT JOIN sys.key_constraints pkconstr
  ON pkconstr.parent_object_id = cols.object_id
  AND ISNULL(pkconstr.type,'') = 'PK'
LEFT JOIN sys.default_constraints defconstr
  ON defconstr.parent_object_id = cols.object_id
  AND defconstr.parent_column_id = cols.column_id
LEFT JOIN sys.computed_columns compCol
  ON compCol.object_id = cols.object_id
WHERE (sc.name + '.' + tbl.name ='{tableName}'
       OR  tbl.name = '{tableName}'
      )
  AND tbl.type = 'U'
  AND tpes.name <> 'sysname'
"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , name => curCol.Name = name.ToString()
            , type_name => curCol.DataType = type_name.ToString()
            , is_nullable => curCol.AllowNulls = (bool)is_nullable
            , is_identity => curCol.IsIdentity = (bool)is_identity
            , seed_value => curCol.IdentitySeed = (int?)seed_value
            , increment_value => curCol.IdentityIncrement = (int?)increment_value
            , primary_key => curCol.IsPrimaryKey = (bool)primary_key
            , default_value =>
                    curCol.DefaultValue = default_value?.ToString().Substring(2, (default_value.ToString().Length) - 4)
            , default_constraint_name => curCol.DefaultConstraintName = default_constraint_name?.ToString()
            , collation_name => curCol.Collation = collation_name?.ToString()
            , computed_column_definition => curCol.ComputedColumn = computed_column_definition?.ToString().Substring(1, (computed_column_definition.ToString().Length) - 2)
             )
            {
                DisableLogging = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromSQLite(string tableName, IConnectionManager connection)
        {
            TableDefinition result = new TableDefinition(tableName);
            TableColumn curCol = null;
            var readMetaSql = new SqlTask($"Read column meta data for table {tableName}",
        $@"PRAGMA table_info(""{tableName}"")"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , cid => {; }
            , name => curCol.Name = name.ToString()
            , type => curCol.DataType = type.ToString()
            , notnull => curCol.AllowNulls = (long)notnull == 1 ? true : false
            , dftl_value => curCol.DefaultValue = dftl_value?.ToString()
            , pk => curCol.IsPrimaryKey = (long)pk == 1 ? true : false
             )
            {
                DisableLogging = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            //if (result.Columns.Where(col => col.IsPrimaryKey).Count() == 1)
            //{
            //    var pkCol = result.Columns.Where(col => col.IsPrimaryKey).First();
            //    if (pkCol.DataType.ToUpper() == "INTEGER")
            //    {
            //        pkCol.IsIdentity = true;
            //        pkCol.IdentityIncrement = 1;
            //        pkCol.IdentitySeed = 1;
            //    }
            //}
            return result;
        }

    }
}
