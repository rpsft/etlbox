using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// A definition for a table in a database
    /// </summary>
    public class TableDefinition
    {
        /// <summary>
        /// The name of the table
        /// </summary>
        public string Name { get; set; }
        
        /// <summary>
        /// The columns of the table
        /// </summary>
        public List<TableColumn> Columns { get; set; }

        /// <summary>
        /// The constraint name for the primary key
        /// </summary>
        public string PrimaryKeyConstraintName { get; set; }

        /// <summary>
        /// The constraint name for the unique columns
        /// </summary>
        public string UniqueKeyConstraintName { get; set; }

        #region Constructors

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

        #endregion

        internal int? IdentityColumnIndex
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

        /// <summary>
        /// Uses the CreateTableTask to create a table based on the current definition.
        /// </summary>
        public void CreateTable() => CreateTableTask.CreateIfNotExists(this);

        /// <summary>
        /// Uses the CreateTableTask to create a table based on the current definition.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        public void CreateTable(IConnectionManager connectionManager) => CreateTableTask.CreateIfNotExists(connectionManager, this);

        /// <summary>
        /// Gather a table definition from an existing table in the database.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">A name of an existing table in the database</param>
        /// <returns></returns>
        public static TableDefinition FromTableName(IConnectionManager connection, string tableName)
        {
            IfTableOrViewExistsTask.ThrowExceptionIfNotExists(connection, tableName);
            ConnectionManagerType connectionType = connection.ConnectionManagerType;
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tableName, connection.QB, connection.QE);

            if (connectionType == ConnectionManagerType.SqlServer)
                return ReadTableDefinitionFromSqlServer(connection, TN);
            else if (connectionType == ConnectionManagerType.SQLite)
                return ReadTableDefinitionFromSQLite(connection, TN);
            else if (connectionType == ConnectionManagerType.MySql)
                return ReadTableDefinitionFromMySqlServer(connection, TN);
            else if (connectionType == ConnectionManagerType.Postgres)
                return ReadTableDefinitionFromPostgres(connection, TN);
            else if (connectionType == ConnectionManagerType.Access)
                return ReadTableDefinitionFromAccess(connection, TN);
            else if (connectionType == ConnectionManagerType.Oracle)
                return ReadTableDefinitionFromOracle(connection, TN);
            else if (connectionType == ConnectionManagerType.Db2)
                return ReadTableDefinitionFromDb2(connection, TN);
            else
                throw new ETLBoxException("Unknown connection type - please pass a valid TableDefinition!");
        }

        private static TableDefinition ReadTableDefinitionFromSqlServer(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask(
$@"
SELECT  cols.name
     , CASE WHEN tpes.name IN ('varchar','char','binary','varbinary') 
            THEN CONCAT ( UPPER(tpes.name)
                        , '('
                        , IIF (cols.max_length = -1, 'MAX', CAST(cols.max_length as varchar(20))) 
                        , ')'
                        )
            WHEN tpes.name IN ('nvarchar','nchar') 
            THEN CONCAT ( UPPER(tpes.name)
                        , '('
                        , IIF (cols.max_length = -1, 'MAX', CAST( (cols.max_length/2) as varchar(20))) 
                        , ')'
                        )
            WHEN tpes.name IN ('decimal','numeric') 
            THEN CONCAT ( UPPER(tpes.name)
                        , '('
                        , cols.precision
                        ,','
                        ,cols.scale, ')'
                        )
            ELSE UPPER(tpes.name)
       END AS type_name
     , cols.is_nullable
     , cols.is_identity
     , ident.seed_value
     , ident.increment_value
     , CONVERT (BIT, CASE WHEN pkidxcols.index_column_id IS NOT NULL THEN 1 ELSE 0 END ) AS primary_key
     , defconstr.definition AS default_value
     , cols.collation_name
     , compCol.definition AS computed_column_definition
     , CONVERT (BIT, CASE WHEN uqidxcols.index_column_id IS NOT NULL THEN 1 ELSE 0 END ) AS is_unique
     , CASE WHEN pkidxcols.index_column_id IS NOT NULL THEN pkidx.name ELSE NULL END AS pkkey_name
     , CASE WHEN uqidxcols.index_column_id IS NOT NULL THEN uqidx.name ELSE NULL END AS uqkey_name
FROM sys.columns cols
INNER JOIN (
    SELECT name, type, object_id, schema_id FROM sys.tables 
    UNION 
    SELECT  name, type, object_id, schema_id FROM sys.views
    ) tbl
    ON cols.object_id = tbl.object_id
INNER JOIN sys.schemas sc
    ON tbl.schema_id = sc.schema_id
INNER JOIN sys.types tpes
    ON tpes.system_type_id = cols.system_type_id
    AND tpes.is_user_defined = 0 
LEFT JOIN sys.identity_columns ident
    ON ident.object_id = cols.object_id
LEFT JOIN sys.indexes pkidx
    ON pkidx.object_id = cols.object_id
    AND pkidx.is_primary_key = 1
LEFT JOIN sys.index_columns pkidxcols
    on pkidxcols.object_id = cols.object_id
    AND pkidxcols.column_id = cols.column_id
    AND pkidxcols.index_id = pkidx.index_id
LEFT JOIN sys.indexes uqidx
    ON uqidx.object_id = cols.object_id
    AND uqidx.is_unique_constraint = 1
LEFT JOIN sys.index_columns uqidxcols
    on uqidxcols.object_id = cols.object_id
    AND uqidxcols.column_id = cols.column_id
    AND uqidxcols.index_id = uqidx.index_id
LEFT JOIN sys.default_constraints defconstr
    ON defconstr.parent_object_id = cols.object_id
    AND defconstr.parent_column_id = cols.column_id
LEFT JOIN sys.computed_columns compCol
    ON compCol.object_id = cols.object_id
    AND compCol.column_id = cols.column_id
WHERE ( CONCAT (sc.name,'.',tbl.name) ='{TN.UnquotatedFullName}' OR  tbl.name = '{TN.UnquotatedFullName}' )
    AND tbl.type IN ('U','V')
    AND tpes.name <> 'sysname'
ORDER BY cols.column_id
"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , name => curCol.Name = name.ToString()
            , type_name => curCol.DataType = type_name.ToString()
            , is_nullable => curCol.AllowNulls = (bool)is_nullable
            , is_identity => curCol.IsIdentity = (bool)is_identity
            , seed_value => curCol.IdentitySeed = (int?)(Convert.ToInt32(seed_value))
            , increment_value => curCol.IdentityIncrement = (int?)(Convert.ToInt32(increment_value))
            , primary_key => curCol.IsPrimaryKey = (bool)primary_key
            , default_value =>
                    curCol.DefaultValue = TryRemoveSingleQuotes(
                            default_value?.ToString().Substring(2, (default_value.ToString().Length) - 4)
                            )
            , collation_name => curCol.Collation = collation_name?.ToString()
            , computed_column_definition => curCol.ComputedColumn = computed_column_definition?.ToString().Substring(1, (computed_column_definition.ToString().Length) - 2)
            , uq_key => curCol.IsUnique = (bool)uq_key
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace( pk_name?.ToString() ) ? result.PrimaryKeyConstraintName : pk_name.ToString()
            , uq_name => result.UniqueKeyConstraintName = String.IsNullOrWhiteSpace(uq_name?.ToString()) ? result.UniqueKeyConstraintName : uq_name.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}"
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromSQLite(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;
            var readMetaSql = new SqlTask(
        $@"PRAGMA table_info(""{TN.UnquotatedFullName}"")"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , cid => {; }
            , name => curCol.Name = name.ToString()
            , type => curCol.DataType = type.ToString()
            , notnull => curCol.AllowNulls = (long)notnull == 0 ? true : false
            , dftl_value => curCol.DefaultValue = TryRemoveSingleQuotes(dftl_value?.ToString())
            , pk => curCol.IsPrimaryKey = (long)pk >= 1 ? true : false
             )
            {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}"
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromMySqlServer(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask(
$@" 
SELECT DISTINCT cols.column_name
  , CASE WHEN cols.data_type IN ('varchar','char','binary') THEN CONCAT (cols.data_type,'(',cols.character_maximum_length, ')')
	     WHEN cols.data_type IN ('decimal') THEN CONCAT (cols.data_type,'(',cols.numeric_precision,',', cols.numeric_scale, ')')
		 ELSE cols.data_type
         END AS 'data_type'
  , CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END AS 'is_nullable'
  , CASE WHEN cols.extra IS NOT NULL AND cols.extra = 'auto_increment' THEN 1 ELSE 0 END AS 'auto_increment'
  , CASE WHEN isnull(k.constraint_name) THEN 0 ELSE 1 END AS 'primary_key'
  , cols.column_default
  , cols.collation_name
  , CASE WHEN cols.generation_expression = '' THEN NULL ELSE cols.generation_expression END AS 'computed_column'
  , CASE WHEN cols.column_comment = '' THEN NULL ELSE cols.column_comment END AS 'comment'
  , CASE WHEN tc_uq.CONSTRAINT_TYPE = 'UNIQUE' THEN 1 ELSE 0 END AS 'is_unique'
  , tc.CONSTRAINT_NAME AS 'pk_name'
  , tc_uq.CONSTRAINT_NAME AS 'uq_constr_name'
  , cols.ORDINAL_POSITION 
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
LEFT JOIN information_schema.TABLE_CONSTRAINTS tc
    ON k.TABLE_SCHEMA = tc.TABLE_SCHEMA
    AND k.TABLE_NAME = tc.TABLE_NAME
    AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME   
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE uq
    ON cols.table_name = uq.table_name
    AND cols.table_schema = uq.table_schema
    AND cols.table_catalog = uq.table_catalog
    AND cols.column_name = uq.column_name
LEFT JOIN information_schema.TABLE_CONSTRAINTS tc_uq
    ON uq.TABLE_SCHEMA = tc_uq.TABLE_SCHEMA
    AND uq.TABLE_NAME = tc_uq.TABLE_NAME
    AND uq.CONSTRAINT_NAME = tc_uq.CONSTRAINT_NAME
    AND tc_uq.CONSTRAINT_TYPE = 'UNIQUE'
WHERE ( cols.table_name = '{TN.UnquotatedFullName}'  OR  CONCAT(cols.table_catalog,'.',cols.table_name) = '{TN.UnquotatedFullName}')
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
            , column_default => curCol.DefaultValue = TryRemoveSingleQuotes(column_default?.ToString())
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
            , comment => curCol.Comment = comment?.ToString()
            , uq_key => curCol.IsUnique = (int)uq_key == 1 ? true : false
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
            , uq_name => result.UniqueKeyConstraintName = String.IsNullOrWhiteSpace(uq_name?.ToString()) ? result.UniqueKeyConstraintName : uq_name.ToString()
            , ignore => { }
             )
            {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}"
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromPostgres(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask(
$@" 
SELECT cols.column_name
,CASE
    WHEN cols.domain_name IS NOT NULL THEN domain_name
    WHEN cols.data_type='character varying' THEN
        CASE WHEN character_maximum_length IS NULL
        THEN 'varchar'
        ELSE 'varchar('||character_maximum_length||')'
        END
    WHEN cols.data_type='character' THEN 'char('||character_maximum_length||')'
    WHEN cols.data_type='numeric' THEN
        CASE WHEN numeric_precision IS NULL
        THEN 'numeric'
        ELSE 'numeric('||numeric_precision||','||numeric_scale||')'
        END
    WHEN LEFT(cols.data_type,4) = 'time' THEN REPLACE(REPLACE(REPLACE(cols.data_type,'without time zone',''), 'with time zone', 'tz'),' ','')
    ELSE cols.data_type
END AS ""datatype""
, CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END AS ""is_nullable""
, CASE WHEN cols.column_default IS NOT NULL AND substring(cols.column_default,0,8) = 'nextval' THEN 1 ELSE 0 END AS ""serial""
, CASE WHEN tccu.column_name IS NULL THEN 0 ELSE 1 END AS ""primary_key""
, cols.column_default
, cols.collation_name
, cols.generation_expression
, CASE WHEN tccu_uq.column_name IS NULL THEN 0 ELSE 1 END AS ""unique_key""
, tccu.constraint_name
, tccu_uq.constraint_name
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN  INFORMATION_SCHEMA.TABLES tbl
    ON cols.table_name = tbl.table_name
    AND cols.table_schema = tbl.table_schema
    AND cols.table_catalog = tbl.table_catalog
LEFT JOIN INFORMATION_SCHEMA.table_constraints tc
    ON cols.table_name = tc.table_name
    AND cols.table_schema = tc.table_schema
    AND cols.table_catalog = tc.table_catalog
    AND tc.constraint_type = 'PRIMARY KEY'
LEFT JOIN information_schema.constraint_column_usage tccu
    ON cols.table_name = tccu.table_name
    AND cols.table_schema = tccu.table_schema
    AND cols.table_catalog = tccu.table_catalog
    AND tccu.constraint_name = tc.constraint_name
    AND tccu.constraint_schema = tc.constraint_schema
    AND tccu.constraint_catalog = tc.constraint_catalog
    AND cols.column_name = tccu.column_name
LEFT JOIN INFORMATION_SCHEMA.table_constraints tc_uq
    ON cols.table_name = tc_uq.table_name
    AND cols.table_schema = tc_uq.table_schema
    AND cols.table_catalog = tc_uq.table_catalog
	AND tc_uq.constraint_type = 'UNIQUE'
LEFT JOIN information_schema.constraint_column_usage tccu_uq
    ON cols.table_name = tccu_uq.table_name
    AND cols.table_schema = tccu_uq.table_schema
    AND cols.table_catalog = tccu_uq.table_catalog
    AND tccu_uq.constraint_name = tc_uq.constraint_name
    AND tccu_uq.constraint_schema = tc_uq.constraint_schema
    AND tccu_uq.constraint_catalog = tc_uq.constraint_catalog
    AND cols.column_name = tccu_uq.column_name   
WHERE(cols.table_name = '{TN.UnquotatedFullName}'  OR  CONCAT(cols.table_schema, '.', cols.table_name) = '{TN.UnquotatedFullName}')
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
            , column_default => curCol.DefaultValue = TryRemoveSingleQuotes(column_default?.ToString().ReplaceIgnoreCase("::character varying", ""))
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
            , uq_key => curCol.IsUnique = (int)uq_key == 1 ? true : false
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
            , uq_name => result.UniqueKeyConstraintName = String.IsNullOrWhiteSpace(uq_name?.ToString()) ? result.UniqueKeyConstraintName : uq_name.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}"
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromOracle(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            //Regarding default values: The issue is described partly here
            //https://stackoverflow.com/questions/46991132/how-to-cast-long-to-varchar2-inline/47041776
            //Oracle uses for some system tables still data type "LONG" (should be replaced by LOB)
            //Unfortunately, there is no build-in function to convert LONG into VARCHAR(2) or a LOB/CLOB or similar
            //When running the select, the default value will be displayed, but ADO.NET can't retrieve the value from the database
            string sql = $@" 
SELECT cols.COLUMN_NAME
, CASE WHEN cols.DATA_TYPE 
            IN ('VARCHAR','CHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2', 'NCHAR2', 'VARCHAR2', 'CHAR2' ) 
        THEN cols.DATA_TYPE || '(' || cols.CHAR_LENGTH || ')'
	   WHEN cols.DATA_TYPE 
            IN ('NUMBER') 
        THEN cols.DATA_TYPE || '(' ||cols.DATA_LENGTH ||',' || 
            CASE WHEN cols.DATA_SCALE IS NULL THEN 127 ELSE cols.DATA_SCALE END
            || ')'
	   ELSE cols.DATA_TYPE
    END AS data_type
, cols.NULLABLE
, cols.IDENTITY_COLUMN
, CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN 'ENABLED' ELSE NULL END as primary_key
, cols.DATA_DEFAULT --not working, see restriction above
, cols.COLLATION 
, cols.DATA_DEFAULT AS generation_expression  --not working, see restriction above
, CASE WHEN cons.CONSTRAINT_TYPE = 'U' THEN 'ENABLED' ELSE NULL END as unique_key
, CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN  cons.CONSTRAINT_NAME ELSE NULL END AS pk_constraint_name
, CASE WHEN cons.CONSTRAINT_TYPE = 'U' THEN  cons.CONSTRAINT_NAME ELSE NULL END AS uk_constraint_name
FROM ALL_TAB_COLUMNS cols
LEFT JOIN (
  SELECT acols.table_name, acols.column_name, acols.position, acons.status, acons.owner, acons.constraint_type
        , acons.CONSTRAINT_NAME
  FROM ALL_CONSTRAINTS acons, ALL_CONS_COLUMNS acols
  WHERE acons.CONSTRAINT_TYPE IN ('U','P')
  AND acons.CONSTRAINT_NAME = acols.CONSTRAINT_NAME
  AND acons.OWNER = acols.OWNER
) cons
ON cons.TABLE_NAME = cols.TABLE_NAME
AND cons.OWNER = cols.OWNER
--AND cons.position = cols.COLUMN_ID
AND cons.column_name = cols.COLUMN_NAME  
WHERE
--cols.TABLE_NAME NOT LIKE 'BIN$%'
--AND cols.OWNER NOT IN ('SYS', 'SYSMAN', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WKSYS', 'WMSYS', 'XDB', 'ORDPLUGINS', 'SYSTEM')
--AND 
    ( cols.TABLE_NAME  = '{TN.UnquotatedFullName}'
      OR (cols.OWNER || '.' || cols.TABLE_NAME ) = '{TN.UnquotatedFullName}'
    )
ORDER BY cols.COLUMN_ID
";

//            LEFT JOIN(
//    SELECT concol.table_name, concol.column_name, concol.position, acons.status, acons.owner, acons.constraint_type
//    FROM ALL_CONS_COLUMNS concol
//    INNER JOIN ALL_CONSTRAINTS acons
//      ON acons.OWNER = concol.OWNER
//      AND acons.CONSTRAINT_TYPE IN ('U','P')
//      AND acons.CONSTRAINT_NAME = concol.CONSTRAINT_NAME
//      AND acons.TABLE_NAME = concol.TABLE_NAME
//WHERE concol.TABLE_NAME NOT LIKE 'BIN$%'
//AND concol.OWNER NOT IN('SYS', 'SYSMAN', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WKSYS', 'WMSYS', 'XDB', 'ORDPLUGINS', 'SYSTEM')
//AND(concol.TABLE_NAME = '{TN.UnquotatedFullName}'
//      OR(concol.OWNER || '.' || concol.TABLE_NAME) = '{TN.UnquotatedFullName}'
//    )
//) cons

            var readMetaSql = new SqlTask(
sql
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , nullable => curCol.AllowNulls = nullable.ToString() == "Y" ? true : false
            , identity_column => curCol.IsIdentity = identity_column?.ToString() == "YES" ? true : false
             , primary_key => curCol.IsPrimaryKey = primary_key?.ToString() == "ENABLED" ? true : false
            , data_default => curCol.DefaultValue = TryRemoveSingleQuotes(data_default?.ToString())
            , collation => curCol.Collation = collation?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
            , uq_key => curCol.IsUnique = uq_key?.ToString() == "ENABLED" ? true : false
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
            , uq_name => result.UniqueKeyConstraintName = String.IsNullOrWhiteSpace(uq_name?.ToString()) ? result.UniqueKeyConstraintName : uq_name.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}"
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromDb2(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            string sql = $@" 
SELECT c.colname AS column_name
     , CASE WHEN c.typename 
            IN ('VARCHAR','CHARACTER','BINARY','VARBINARY','CLOB','BLOB','DBCLOB','GRAPHIC','VARGRAPHIC' ) 
       THEN c.typename || '(' || c.length || ')'  
	   WHEN c.typename 
            IN ('DECIMAL','NUMERIC','DECFLOAT','REAL','DOUBLE') 
       THEN c.typename || '(' || c.length ||',' || c.scale || ')'
	   ELSE c.typename
	   END AS data_type 
     , CASE WHEN c.nulls = 'Y' THEN 1 ELSE 0 END AS nullable
     , CASE WHEN c.identity ='Y' THEN 1 ELSE 0 END AS is_identity
     , CASE WHEN i.uniquerule ='P' THEN 1 ELSE 0 END AS is_primary
     , c.default AS default_value
     , c.collationname AS collation     
     --, c.generated as  generation_expression
     , c.text as computed_formula
    , CASE WHEN i.uniquerule ='U' THEN 1 ELSE 0 END AS is_unique
     , c.remarks as description
     , CASE WHEN i.uniquerule ='P' THEN i.indname ELSE NULL END AS pk_name
     , CASE WHEN i.uniquerule ='U' THEN i.indname ELSE NULL END AS uk_name
FROM syscat.columns c
INNER JOIN syscat.tables t on 
      t.tabschema = c.tabschema and t.tabname = c.tabname
LEFT JOIN (
    SELECT ix.uniquerule, ix.tabschema, ix.tabname, idxu.colname, ix.indname
    FROM syscat.indexes ix
    INNER JOIN syscat.indexcoluse idxu
    ON idxu.indname = ix.indname
    AND idxu.indschema = ix.indschema
    ) i
ON i.tabschema = c.tabschema 
    AND i.tabname = c.tabname
    AND i.colname = c.colname     
WHERE t.type IN ('V','T')
AND ( t.tabname = '{TN.UnquotatedFullName}'
      OR ( TRIM(t.tabschema) || '.' || t.tabname = '{TN.UnquotatedFullName}' )
    )
ORDER BY c.colno;
";            

            var readMetaSql = new SqlTask(
sql
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , is_identity => curCol.IsIdentity = (int)is_identity == 1 ? true : false
            , is_primary => curCol.IsPrimaryKey = (int)is_primary == 1 ? true : false
            , default_value => curCol.DefaultValue = TryRemoveSingleQuotes(default_value?.ToString())
            , collation => curCol.Collation = collation?.ToString()
            , computed_formula => curCol.ComputedColumn = computed_formula?.ToString()
            , is_unique => curCol.IsUnique = (int)is_unique == 1 ? true : false
            , remarks => curCol.Comment = remarks?.ToString()
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
            , uq_name => result.UniqueKeyConstraintName = String.IsNullOrWhiteSpace(uq_name?.ToString()) ? result.UniqueKeyConstraintName : uq_name.ToString()
             )
            {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}"
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        private static TableDefinition ReadTableDefinitionFromAccess(IConnectionManager connection, ObjectNameDescriptor TN)
        {
            var connDbObject = connection as IConnectionManagerDbObjects;
            return connDbObject?.ReadTableDefinition(TN);
        }

        private static string TryRemoveSingleQuotes(string value)
        {
            if (!string.IsNullOrEmpty(value) && value.StartsWith("'") && value.EndsWith("'"))
                return value.TrimStart('\'').TrimEnd('\'');
            else
                return value;
        }
    }
}
