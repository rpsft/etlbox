using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System.Collections.Generic;
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
            IfTableOrViewExistsTask.ThrowExceptionIfNotExists(connection, tableName);
            ConnectionManagerType connectionType = ConnectionManagerSpecifics.GetType(connection);

            if (connectionType == ConnectionManagerType.SqlServer)
                return ReadTableDefinitionFromSqlServer(tableName, connection);
            else if (connectionType == ConnectionManagerType.SQLite)
                return ReadTableDefinitionFromSQLite(tableName, connection);
            else if (connectionType == ConnectionManagerType.MySql)
                return ReadTableDefinitionFromMySqlServer(tableName, connection);
            else if (connectionType == ConnectionManagerType.Postgres)
                return ReadTableDefinitionFromPostgres(tableName, connection);
            else
                throw new ETLBoxException("Unknown connection type - please pass a valid TableDefinition!");
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
WHERE ( CONCAt (sc.name,'.',tbl.name) ='{tableName}' OR  tbl.name = '{tableName}' )
    AND tbl.type = 'U'
    AND tpes.name <> 'sysname'
ORDER BY cols.column_id
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
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromMySqlServer(string tableName, IConnectionManager connection)
        {
            TableDefinition result = new TableDefinition(tableName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {tableName}",
$@" 
SELECT cols.column_name
  , cols.data_type
  , CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END AS 'is_nullable'
  , CASE WHEN cols.extra IS NOT NULL AND cols.extra = 'auto_increment' THEN 1 ELSE 0 END AS 'auto_increment'
  , CASE WHEN isnull(k.constraint_name) THEN 0 ELSE 1 END AS 'primary_key'
  , cols.column_default
  , cols.collation_name
  , cols.generation_expression
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN  INFORMATION_SCHEMA.TABLES tbl
    ON cols.table_name = tbl.table_name
    AND cols.table_schema = tbl.table_schema
    AND cols.table_catalog = tbl.table_catalog
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
    ON cols.table_name = k.table_name
    AND cols.table_schema = k.table_schema
    AND cols.table_catalog = k.table_catalog
    AND cols.column_name = k.column_name
    AND k.constraint_name = 'PRIMARY'
WHERE ( cols.table_name = '{tableName}'  OR  CONCAT(cols.table_catalog,'.',cols.table_name) = '{tableName}')
    AND cols.table_schema = DATABASE()
ORDER BY cols.ordinal_position
"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , auto_increment => curCol.IsIdentity = (int)auto_increment == 1 ? true : false
             , primary_key => curCol.IsPrimaryKey = (int)primary_key == 1 ? true : false
            , column_default => curCol.DefaultValue = column_default?.ToString()
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromPostgres(string tableName, IConnectionManager connection)
        {
            TableDefinition result = new TableDefinition(tableName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask($"Read column meta data for table {tableName}",
$@" 
SELECT cols.column_name
, cols.data_type
, CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END AS ""is_nullable""
, CASE WHEN cols.column_default IS NOT NULL AND substring(cols.column_default,0,8) = 'nextval' THEN 1 ELSE 0 END AS ""serial""
, CASE WHEN k.constraint_name IS NULL THEN 0 ELSE 1 END AS ""primary_key""
, cols.column_default
, cols.collation_name
, cols.generation_expression
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN  INFORMATION_SCHEMA.TABLES tbl
    ON cols.table_name = tbl.table_name
    AND cols.table_schema = tbl.table_schema
    AND cols.table_catalog = tbl.table_catalog
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
    ON cols.table_name = k.table_name
    AND cols.table_schema = k.table_schema
    AND cols.table_catalog = k.table_catalog
    AND cols.column_name = k.column_name
WHERE(cols.table_name = '{tableName}'  OR  CONCAT(cols.table_schema, '.', cols.table_name) = '{tableName}')
    AND cols.table_catalog = CURRENT_DATABASE()
ORDER BY cols.ordinal_position
"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , serial => curCol.IsIdentity = (int)serial == 1 ? true : false
             , primary_key => curCol.IsPrimaryKey = (int)primary_key == 1 ? true : false
            , column_default => curCol.DefaultValue = column_default?.ToString().ReplaceIgnoreCase("::character varying", "")
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }
    }
}
