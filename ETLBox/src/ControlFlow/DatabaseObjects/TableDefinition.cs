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
        public IList<TableColumn> Columns { get; set; }

        /// <summary>
        /// The constraint name for the primary key
        /// </summary>
        public string PrimaryKeyConstraintName { get; set; }

        //public string UniqueKeyConstraintName { get; set; }
        public ICollection<UniqueKeyConstraint> UniqueKeyConstraints { get; set; }

        public ICollection<ForeignKeyConstraint> ForeignKeyConstraints { get; set; }

        #region Constructors

        public TableDefinition() {
            Columns = new List<TableColumn>();
        }

        public TableDefinition(string name) : this() {
            Name = name;
        }

        public TableDefinition(string name, List<TableColumn> columns) : this(name) {
            Columns = columns;
        }

        #endregion

        internal int? IdentityColumnIndex {
            get {
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
        /// <param name="connection">The connection manager of the database you want to connect</param>
        /// <param name="tableName">A name of an existing table in the database</param>
        /// <param name="readConstraints">If set to true, information about the Unique and Foreign Key constraints are also gathered.</param>
        /// <returns></returns>
        public static TableDefinition FromTableName(IConnectionManager connection, string tableName, bool readConstraints = true) {
            if (!IfTableOrViewExistsTask.IsExisting(connection, tableName))
                throw new ETLBoxException($"A table or view '{tableName}' does not exist in the database!");
            ConnectionManagerType connectionType = connection.ConnectionManagerType;
            ObjectNameDescriptor TN = new ObjectNameDescriptor(tableName, connection.QB, connection.QE);
            TableDefinition definition;
            if (connectionType == ConnectionManagerType.SQLite)
                readConstraints = false;

            if (connectionType == ConnectionManagerType.SqlServer) {
                definition = ReadBasicTableDefinitionSqlServer(connection, TN);
                if (readConstraints) {
                    AddUniqueConstraintsToDefinition(connection, TN, definition);
                    AddForeignKeyConstraints(connection, TN, definition);
                }
                return definition;
            } else if (connectionType == ConnectionManagerType.SQLite) {
                definition = ReadBasicTableDefinitionSQLIte(connection, TN);
                return definition;
            } else if (connectionType == ConnectionManagerType.Postgres) {
                definition = ReadBasicTableDefinitionFromPostgres(connection, TN);
                if (readConstraints) {
                    AddUniqueConstraintsToDefinition(connection, TN, definition);
                    AddForeignKeyConstraints(connection, TN, definition);
                }
                return definition;
            } else if (connectionType == ConnectionManagerType.MySql) {
                definition = ReadTableDefinitionFromMySqlServer(connection, TN);
                if (readConstraints) {
                    AddUniqueConstraintsToDefinition(connection, TN, definition);
                    AddForeignKeyConstraints(connection, TN, definition);
                }
                return definition;
            } else if (connectionType == ConnectionManagerType.Access)
                return ReadTableDefinitionFromAccess(connection, TN);
            else if (connectionType == ConnectionManagerType.Oracle) {
                definition = ReadTableDefinitionFromOracle(connection, TN);
                if (readConstraints) {
                    AddUniqueConstraintsToDefinition(connection, TN, definition);
                    AddForeignKeyConstraints(connection, TN, definition);
                }
                return definition;
            } else if (connectionType == ConnectionManagerType.Db2) {
                definition = ReadTableDefinitionFromDb2(connection, TN);
                if (readConstraints) {
                    AddUniqueConstraintsToDefinition(connection, TN, definition);
                    AddForeignKeyConstraints(connection, TN, definition);
                }
                return definition;
            } else
                throw new ETLBoxException("Unknown connection type - please pass a valid connection manager or provide your own TableDefinition!");
        }

        static string SqlServerBasicSql(bool IsOdbc = false) {
            return $@"
SELECT cols.name
     , CASE
           WHEN tpes.name IN ('varchar', 'char', 'binary', 'varbinary')
               THEN CONCAT(UPPER(tpes.name)
               , '('
               , IIF(cols.max_length = -1, 'MAX', CAST(cols.max_length as varchar(20)))
               , ')'
               )
           WHEN tpes.name IN ('nvarchar', 'nchar')
               THEN CONCAT(UPPER(tpes.name)
               , '('
               , IIF(cols.max_length = -1, 'MAX', CAST((cols.max_length / 2) as varchar(20)))
               , ')'
               )
           WHEN tpes.name IN ('decimal', 'numeric')
               THEN CONCAT(UPPER(tpes.name)
               , '('
               , cols.precision
               , ','
               , cols.scale, ')'
               )
           ELSE UPPER(tpes.name)
    END                                                               AS type_name
     , cols.is_nullable
     , cols.is_identity
     , ident.seed_value
     , ident.increment_value
     , CONVERT(BIT, IIF(pkidxcols.index_column_id IS NOT NULL, 1, 0)) AS primary_key
     , defconstr.definition                                           AS default_value
     , cols.collation_name
     , compCol.definition                                             AS computed_column_definition
     , IIF(pkidxcols.index_column_id IS NOT NULL, pkidx.name, NULL)   AS pkkey_name
FROM sys.columns cols
         INNER JOIN (
    SELECT name, type, object_id, schema_id
    FROM sys.tables
    UNION
    SELECT name, type, object_id, schema_id
    FROM sys.views
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
         LEFT JOIN sys.default_constraints defconstr
                   ON defconstr.parent_object_id = cols.object_id
                       AND defconstr.parent_column_id = cols.column_id
         LEFT JOIN sys.computed_columns compCol
                   ON compCol.object_id = cols.object_id
                       AND compCol.column_id = cols.column_id
WHERE (CONCAT(sc.name, '.', tbl.name) = { (!IsOdbc ? "@TN1" : "?") } OR tbl.name = { (!IsOdbc ? "@TN2" : "?") })
  AND tbl.type IN ('U', 'V')
  AND tpes.name <> 'sysname'
ORDER BY cols.column_id
";
        }
        private static TableDefinition ReadBasicTableDefinitionSqlServer(IConnectionManager connection, ObjectNameDescriptor TN) {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask(SqlServerBasicSql(connection.IsOdbcOrOleDbConnection)
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
                    curCol.DefaultValue = TryRemoveTrailingSingleQuotes(
                            default_value?.ToString().Substring(2, (default_value.ToString().Length) - 4)
                            )
            , collation_name => curCol.Collation = collation_name?.ToString()
            , computed_column_definition => curCol.ComputedColumn = computed_column_definition?.ToString().Substring(1, (computed_column_definition.ToString().Length) - 2)
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
             ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}",
                Parameter = new[] {                    
                    new QueryParameter("TN1", "VARCHAR(1000)", TN.UnquotatedFullName),
                    new QueryParameter("TN2", "VARCHAR(1000)", TN.UnquotatedFullName),
                }
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        static string SqlServerUniqueConstraintSql(bool IsOdbc = false) {
            return $@"
SELECT CASE WHEN uqidxcols.index_column_id IS NOT NULL THEN uqidx.name ELSE NULL END AS uqkey_name
     , cols.name
FROM sys.columns cols
         INNER JOIN(
    SELECT name, type, object_id, schema_id
    FROM sys.tables
    UNION
    SELECT name, type, object_id, schema_id
    FROM sys.views
) tbl
                   ON cols.object_id = tbl.object_id
         INNER JOIN sys.schemas sc
                    ON tbl.schema_id = sc.schema_id
         INNER JOIN sys.types tpes
                    ON tpes.system_type_id = cols.system_type_id
                        AND tpes.is_user_defined = 0
         INNER JOIN sys.indexes uqidx
                    ON uqidx.object_id = cols.object_id
                        AND uqidx.is_unique_constraint = 1
         INNER JOIN sys.index_columns uqidxcols
                    on uqidxcols.object_id = cols.object_id
                        AND uqidxcols.column_id = cols.column_id
                        AND uqidxcols.index_id = uqidx.index_id
WHERE (CONCAT(sc.name, '.', tbl.name) =  { (!IsOdbc ? "@TN1" : "?") } OR tbl.name =  { (!IsOdbc ? "@TN2" : "?") })
  AND tbl.type IN ('U', 'V')
  AND tpes.name <> 'sysname'
ORDER BY uqidx.name, cols.column_id
";
        }

        static string PostgresUniqueConstraintSql(bool IsOdbc = false) {
            return $@"
SELECT tccu_uq.constraint_name  AS ""constraint_name""
    , cols.column_name         AS ""column_name""
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN  INFORMATION_SCHEMA.table_constraints tc_uq
           ON cols.table_name = tc_uq.table_name
               AND cols.table_schema = tc_uq.table_schema
               AND cols.table_catalog = tc_uq.table_catalog
               AND tc_uq.constraint_type = 'UNIQUE'
INNER JOIN  information_schema.constraint_column_usage tccu_uq
           ON cols.table_name = tccu_uq.table_name
               AND cols.table_schema = tccu_uq.table_schema
               AND cols.table_catalog = tccu_uq.table_catalog
               AND tccu_uq.constraint_name = tc_uq.constraint_name
               AND tccu_uq.constraint_schema = tc_uq.constraint_schema
               AND tccu_uq.constraint_catalog = tc_uq.constraint_catalog
               AND cols.column_name = tccu_uq.column_name
WHERE(cols.table_name = { (!IsOdbc ? "@TN1" : "?") }  OR  CONCAT(cols.table_schema, '.', cols.table_name) = { (!IsOdbc ? "@TN2" : "?") })
  AND cols.table_catalog = CURRENT_DATABASE()
ORDER BY tccu_uq.constraint_name, cols.ordinal_position
";
        }

        static string MySqlUniqueConstraintSql(bool IsOdbc = false) {
            return $@"
SELECT tc_uq.CONSTRAINT_NAME AS 'uq_constr_name'
     , cols.column_name
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE uq
           ON cols.table_name = uq.table_name
               AND cols.table_schema = uq.table_schema
               AND cols.table_catalog = uq.table_catalog
               AND cols.column_name = uq.column_name
INNER JOIN information_schema.TABLE_CONSTRAINTS tc_uq
           ON uq.TABLE_SCHEMA = tc_uq.TABLE_SCHEMA
               AND uq.TABLE_NAME = tc_uq.TABLE_NAME
               AND uq.CONSTRAINT_NAME = tc_uq.CONSTRAINT_NAME
               AND tc_uq.CONSTRAINT_TYPE = 'UNIQUE'
WHERE (cols.table_name = { (!IsOdbc ? "@TN1" : "?") } OR CONCAT(cols.table_catalog, '.', cols.table_name) = { (!IsOdbc ? "@TN2" : "?") })
  AND cols.table_schema = DATABASE()
ORDER BY uq.CONSTRAINT_NAME, cols.ordinal_position
";
        }

        static string MariaDbUniqueConstraintSql(bool IsOdbc = false) {
            return $@"
SELECT CAST(a.uq_constr_name AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci' AS 'uq_constr_name',
    CAST(a.column_name AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci' AS 'column_name'
FROM ( 
" + MySqlUniqueConstraintSql(IsOdbc) + $@"
) a";
        }

        static string OracleUniqueConstraintSql(bool IsOdbc = false) {
            return $@"
SELECT cons.CONSTRAINT_NAME
     , cols.COLUMN_NAME
FROM ALL_TAB_COLUMNS cols
INNER JOIN(
             SELECT concol.table_name,
                    concol.column_name,
                    concol.position,
                    acons.status,
                    acons.owner,
                    acons.constraint_type,
                    acons.CONSTRAINT_NAME
             FROM ALL_CONS_COLUMNS concol
             INNER JOIN ALL_CONSTRAINTS acons
                        ON acons.OWNER = concol.OWNER
                            AND acons.CONSTRAINT_TYPE IN ('U')
                            AND acons.CONSTRAINT_NAME = concol.CONSTRAINT_NAME
                            AND acons.TABLE_NAME = concol.TABLE_NAME
         ) cons
         ON cons.TABLE_NAME = cols.TABLE_NAME
             AND cons.OWNER = cols.OWNER
             --AND cons.position = cols.COLUMN_ID
             AND cons.column_name = cols.COLUMN_NAME
WHERE cols.TABLE_NAME NOT LIKE 'BIN$%'
  AND cols.OWNER NOT IN
      ('SYS', 'SYSMAN', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WKSYS', 'WMSYS', 'XDB', 'ORDPLUGINS',
       'SYSTEM')
  AND (cols.TABLE_NAME = { (!IsOdbc ? ":TN1" : "?") }
    OR (cols.OWNER || '.' || cols.TABLE_NAME) = { (!IsOdbc ? ":TN2" : "?") }
    )
ORDER BY cons.CONSTRAINT_NAME, cols.COLUMN_ID
";
        }

        static string Db2UniqueConstraintSql(bool IsOdbc = false) {
            return $@"
SELECT CASE WHEN i.NON_UNIQUE = 0 THEN i.INDEX_NAME ELSE NULL END AS uk_name
    , c.COLUMN_NAME AS column_name
FROM SYSIBM.SQLCOLUMNS c
INNER JOIN SYSIBM.SQLTABLES t on
      t.TABLE_SCHEM = c.TABLE_SCHEM and t.TABLE_NAME = c.TABLE_NAME
LEFT JOIN SYSIBM.SQLPRIMARYKEYS pk
    ON pk.TABLE_NAME = c.TABLE_NAME
    AND pk.TABLE_SCHEM = c.TABLE_SCHEM
    AND pk.COLUMN_NAME = c.COLUMN_NAME
INNER JOIN SYSIBM.SQLSTATISTICS i
    ON i.TABLE_NAME = c.TABLE_NAME
    AND i.TABLE_SCHEM = c.TABLE_SCHEM
    AND i.COLUMN_NAME = c.COLUMN_NAME
WHERE t.TABLE_TYPE IN ('VIEW','TABLE')
AND ( t.TABLE_NAME = { (!IsOdbc ? "@TN1" : "?") }
      OR ( TRIM(t.TABLE_SCHEM) || '.' || t.TABLE_NAME = { (!IsOdbc ? "@TN2" : "?") } )
    )
AND (pk.PK_NAME IS NULL OR pk.PK_NAME <> i.INDEX_NAME)
ORDER BY i.Index_name, c.ORDINAL_POSITION;
";
        }

        private static void AddUniqueConstraintsToDefinition(IConnectionManager connection, ObjectNameDescriptor TN, TableDefinition definition) {
            List<UniqueKeyConstraint> newConstraints = new List<UniqueKeyConstraint>();
            UniqueKeyConstraint curConstraint = new UniqueKeyConstraint();

            string sql = "";
            if (connection.ConnectionManagerType == ConnectionManagerType.SqlServer)
                sql = SqlServerUniqueConstraintSql(connection.IsOdbcOrOleDbConnection);
            else if (connection.ConnectionManagerType == ConnectionManagerType.Postgres)
                sql = PostgresUniqueConstraintSql(connection.IsOdbcOrOleDbConnection);
            else if (connection.ConnectionManagerType == ConnectionManagerType.MySql) {
                if (connection.Compatibility?.ToLower() == "mariadb")
                    sql = MariaDbUniqueConstraintSql(connection.IsOdbcOrOleDbConnection);
                else 
                    sql = MySqlUniqueConstraintSql(connection.IsOdbcOrOleDbConnection);
            } else if (connection.ConnectionManagerType == ConnectionManagerType.Oracle)
                sql = OracleUniqueConstraintSql(connection.IsOdbcOrOleDbConnection);
            else if (connection.ConnectionManagerType == ConnectionManagerType.Db2)
                sql = Db2UniqueConstraintSql(connection.IsOdbcOrOleDbConnection);
            else
                throw new ETLBoxException("Unknown connection type - please pass a valid connection manager or provide or disable constraint reading!");

            var readMetaSql = new SqlTask(sql
                  , () => { }
                  , () => { }
                  , uniqueKeyName => {
                      if (string.IsNullOrWhiteSpace(uniqueKeyName?.ToString())) return;
                      if (curConstraint.ColumnNames?.Count > 0 && curConstraint.ConstraintName != uniqueKeyName.ToString()) {
                          newConstraints.Add(curConstraint);
                          curConstraint = new UniqueKeyConstraint();
                      }
                      curConstraint.ConstraintName = uniqueKeyName.ToString();
                  }
                  , colName => {
                      if (string.IsNullOrWhiteSpace(colName?.ToString())) return;
                      if (curConstraint.ColumnNames == null)
                          curConstraint.ColumnNames = new List<string>();
                      curConstraint.ColumnNames.Add(colName.ToString());
                  }
           ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read unique constraint meta data for table {TN.ObjectName}",
                Parameter = new[] { 
                    new QueryParameter("TN1", "VARCHAR(1000)", TN.UnquotatedFullName),
                    new QueryParameter("TN2", "VARCHAR(1000)", TN.UnquotatedFullName)
                }
            };
            readMetaSql.ExecuteReader();
            if (curConstraint.ColumnNames?.Count > 0) newConstraints.Add(curConstraint);
            if (newConstraints?.Count > 0)
                definition.UniqueKeyConstraints = newConstraints;
        }

        static string SqlServerForeignKeySql(bool IsOdbc = false) {
            return $@"
SELECT fkcon.constraintName AS fk_constraint_name
     , cols.name
     , fkcon.refTableName   AS fk_reference_table
     , fkcon.refColumnName  AS fk_reference_column

FROM sys.columns cols
         INNER JOIN(
    SELECT name, type, object_id, schema_id
    FROM sys.tables
    UNION
    SELECT name, type, object_id, schema_id
    FROM sys.views
) tbl
                   ON cols.object_id = tbl.object_id
         INNER JOIN sys.schemas sc
                    ON tbl.schema_id = sc.schema_id
         INNER JOIN sys.types tpes
                    ON tpes.system_type_id = cols.system_type_id
                        AND tpes.is_user_defined = 0
         INNER JOIN(
    SELECT '[' + fk_sc.name + '].[' + fk_t.name + ']' AS refTableName,
           fk_c.name                                  AS refColumnName,
           fk_c.column_id                             AS refColumnId,
           fk_const.name                              AS constraintName,
           fk.parent_object_id                        AS colObjectId,
           fk.parent_column_id                        AS colColumnId
    FROM sys.foreign_key_columns fk
             INNER JOIN sys.columns AS fk_c
                        ON fk.referenced_object_id = fk_c.object_id and fk.referenced_column_id = fk_c.column_id
             INNER JOIN sys.tables AS fk_t
                        ON fk.referenced_object_id = fk_t.object_id
             INNER JOIN sys.schemas fk_sc
                        ON fk_t.schema_id = fk_sc.schema_id
             INNER JOIN sys.objects fk_const
                        ON fk_const.object_id = fk.constraint_object_id
) fkcon
                   ON fkcon.colColumnId = cols.column_id
                       AND fkcon.colObjectId = cols.object_id
WHERE (CONCAT(sc.name, '.', tbl.name) = { (!IsOdbc ? "@TN1" : "?") } OR tbl.name = { (!IsOdbc ? "@TN2" : "?") })
  AND tbl.type IN ('U', 'V')
  AND tpes.name <> 'sysname'
ORDER BY fkcon.constraintName, fkcon.refTableName, fkcon.refColumnId, cols.column_id
";
        }

        static string PostgresForeignKeySql(bool IsOdbc = false) {
            return $@"
SELECT c.constraint_name
     , cols.column_name
     , CONCAT('""', kcu2.table_schema, '"".""', kcu2.table_name, '""') AS refTableName
     , kcu2.column_name                                            AS refColumnName
FROM information_schema.referential_constraints c
join information_schema.key_column_usage kcu
     on kcu.constraint_name = c.constraint_name
         and kcu.constraint_catalog = c.constraint_catalog
         and kcu.constraint_schema = c.constraint_schema
join information_schema.key_column_usage kcu2
     on kcu2.ordinal_position = kcu.position_in_unique_constraint
         and kcu2.constraint_name = c.unique_constraint_name
JOIN information_schema.Columns cols
     ON cols.column_name = kcu.column_name
         AND kcu.table_name = cols.table_name
         AND kcu.table_schema = cols.table_schema
         AND kcu.table_catalog = cols.table_catalog
WHERE (cols.table_name = { (!IsOdbc ? "@TN1" : "?") } OR CONCAT(cols.table_schema, '.', cols.table_name) = { (!IsOdbc ? "@TN2" : "?") })
  AND cols.table_catalog = CURRENT_DATABASE()
ORDER BY c.constraint_name, kcu2.table_schema, kcu2.table_name, kcu2.ordinal_position, cols.ordinal_position
";
        }

        static string MySqlForeignKeySql(bool IsOdbc = false) {
            return $@"
SELECT fk.CONSTRAINT_NAME                                  AS 'fk_constraint_name'
     , cols.column_name
     , CONCAT('`', fk.REFERENCED_TABLE_SCHEMA, '`.`',
              fk.REFERENCED_TABLE_NAME, '`') AS 'fk_reference_table'
     , fk.REFERENCED_COLUMN_NAME                           AS 'fk_reference_column'
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
           ON cols.table_name = fk.table_name
               AND cols.table_schema = fk.table_schema
               AND cols.table_catalog = fk.table_catalog
               AND cols.column_name = fk.column_name
               AND fk.CONSTRAINT_NAME <> 'PRIMARY'
WHERE (cols.table_name = { (!IsOdbc ? "@TN1" : "?") } OR
       CONCAT(cols.table_catalog, '.', cols.table_name) = { (!IsOdbc ? "@TN2" : "?") })
  AND cols.table_schema = DATABASE()
ORDER BY fk.constraint_name, fk.referenced_table_name, fk.POSITION_IN_UNIQUE_CONSTRAINT, cols.ordinal_position
";
        }

        static string MariaDbForeignConstraintSql(bool IsOdbc = false) {
            return $@"
SELECT CAST(a.fk_constraint_name AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci' AS 'fk_constraint_name',
       CAST(a.column_name AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci' AS 'column_name',
       CAST(a.fk_reference_table AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci' AS 'fk_reference_table',
       CAST(a.fk_reference_column AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci' AS 'fk_reference_column'
FROM ( 
" + MySqlForeignKeySql(IsOdbc) + $@"
) a";
        }

        static string OracleForeignKeySql(bool IsOdbc = false) {
            return $@"
SELECT cons.CONSTRAINT_NAME
     , cols.COLUMN_NAME
     , cons.r_table_name
     , cons.r_column_name
FROM ALL_TAB_COLUMNS cols
INNER JOIN(
              SELECT a.table_name
                   , a.column_name
                   , a.POSITION
                   , c.status
                   , c.owner
                   , c.CONSTRAINT_TYPE
                   , c.CONSTRAINT_NAME
                   , c_pk.table_name AS r_table_name
                   , b.column_name   AS r_column_name
                   , b.POSITION      AS r_position
              FROM all_cons_columns a
              JOIN      all_constraints c
                        ON a.owner = c.owner
                            AND a.constraint_name = c.constraint_name
              LEFT JOIN all_constraints c_pk
                        ON c.r_owner = c_pk.owner
                            AND c.r_constraint_name = c_pk.constraint_name
              LEFT JOIN all_cons_columns b
                        ON C_PK.owner = b.owner
                            AND C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION
              WHERE c.constraint_type = 'R'
          ) cons
          ON cons.TABLE_NAME = cols.TABLE_NAME
              AND cons.OWNER = cols.OWNER
              --AND cons.position = cols.COLUMN_ID
              AND cons.column_name = cols.COLUMN_NAME
WHERE cols.TABLE_NAME NOT LIKE 'BIN$%'
  AND cols.OWNER NOT IN
      ('SYS', 'SYSMAN', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WKSYS', 'WMSYS', 'XDB', 'ORDPLUGINS',
       'SYSTEM')
  AND (cols.TABLE_NAME = { (!IsOdbc ? ":TN1" : " ? ") }
    OR (cols.OWNER || '.' || cols.TABLE_NAME) = { (!IsOdbc ? ":TN2" : "?") }
    )
ORDER BY cons.CONSTRAINT_NAME, cons.r_table_name, cons.r_position, cols.COLUMN_ID
";
        }

        static string Db2ForeignKeySql(bool IsOdbc = false) {
            return $@"
SELECT fk.FK_NAME
     , c.COLUMN_NAME AS column_name
     , fk.PKTABLE_NAME
     , fk.PKCOLUMN_NAME     
FROM SYSIBM.SQLCOLUMNS c
INNER JOIN SYSIBM.SQLTABLES t
           on
               t.TABLE_SCHEM = c.TABLE_SCHEM and t.TABLE_NAME = c.TABLE_NAME
INNER JOIN SYSIBM.SQLFOREIGNKEYS fk
           ON fk.FKTABLE_NAME = c.TABLE_NAME
               AND fk.FKTABLE_SCHEM = c.TABLE_SCHEM
               AND fk.FKCOLUMN_NAME = c.COLUMN_NAME
WHERE t.TABLE_TYPE IN ('VIEW', 'TABLE')
  AND (t.TABLE_NAME = { (!IsOdbc ? "@TN1" : "?") }
    OR (TRIM(t.TABLE_SCHEM) || '.' || t.TABLE_NAME = { (!IsOdbc ? "@TN2" : "?") })
    )
ORDER BY fk.FK_NAME, fk.PKTABLE_NAME, fk.KEY_SEQ, c.ORDINAL_POSITION;
";
        }

        private static void AddForeignKeyConstraints(IConnectionManager connection, ObjectNameDescriptor TN, TableDefinition definition) {
            List<ForeignKeyConstraint> newConstraints = new List<ForeignKeyConstraint>();
            ForeignKeyConstraint curConstraint = new ForeignKeyConstraint();

            string sql = "";
            if (connection.ConnectionManagerType == ConnectionManagerType.SqlServer)
                sql = SqlServerForeignKeySql(connection.IsOdbcOrOleDbConnection);
            else if (connection.ConnectionManagerType == ConnectionManagerType.Postgres)
                sql = PostgresForeignKeySql(connection.IsOdbcOrOleDbConnection);
            else if (connection.ConnectionManagerType == ConnectionManagerType.MySql) {
                if (connection.Compatibility?.ToLower() == "mariadb")
                    sql = MariaDbForeignConstraintSql(connection.IsOdbcOrOleDbConnection);
                else
                    sql = MySqlForeignKeySql(connection.IsOdbcOrOleDbConnection);
            } else if (connection.ConnectionManagerType == ConnectionManagerType.Oracle)
                sql = OracleForeignKeySql(connection.IsOdbcOrOleDbConnection);
            else if (connection.ConnectionManagerType == ConnectionManagerType.Db2)
                sql = Db2ForeignKeySql(connection.IsOdbcOrOleDbConnection);
            else
                throw new ETLBoxException("Unknown connection type - please pass a valid connection manager or provide or disable constraint reading!");

            var readMetaSql = new SqlTask(sql
                  , () => { }
                  , () => { }
                  , fk_constraint_name => {
                      if (string.IsNullOrWhiteSpace(fk_constraint_name?.ToString())) return;
                      if (curConstraint.ColumnNames?.Count > 0 && curConstraint.ConstraintName != fk_constraint_name.ToString()) {
                          newConstraints.Add(curConstraint);
                          curConstraint = new ForeignKeyConstraint();
                      }
                      curConstraint.ConstraintName = fk_constraint_name.ToString();
                  }
                  , colName => {
                      if (string.IsNullOrWhiteSpace(colName?.ToString())) return;
                      if (curConstraint.ColumnNames == null)
                          curConstraint.ColumnNames = new List<string>();
                      curConstraint.ColumnNames.Add(colName.ToString());
                  }
                  , fk_reference_table => {
                      if (string.IsNullOrWhiteSpace(fk_reference_table?.ToString())) return;
                      curConstraint.ReferenceTableName = fk_reference_table.ToString();
                  }
                  , fk_reference_column => {
                      if (string.IsNullOrWhiteSpace(fk_reference_column?.ToString())) return;
                      if (curConstraint.ReferenceColumnNames == null)
                          curConstraint.ReferenceColumnNames = new List<string>();
                      curConstraint.ReferenceColumnNames.Add(fk_reference_column.ToString());
                  }
           ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read foreign key constraints meta data for table {TN.ObjectName}",
                Parameter = new[] { 
                    new QueryParameter("TN1", "VARCHAR(1000)", TN.UnquotatedFullName),
                    new QueryParameter("TN2", "VARCHAR(1000)", TN.UnquotatedFullName)
                }
            };
            readMetaSql.ExecuteReader();
            if (curConstraint.ColumnNames?.Count > 0) newConstraints.Add(curConstraint);
            if (newConstraints?.Count > 0)
                definition.ForeignKeyConstraints = newConstraints;
        }


        static string SQLiteBasicSql(ObjectNameDescriptor TN) {
            return $@"PRAGMA table_info(""{TN.UnquotatedFullName}"")";
        }

        private static TableDefinition ReadBasicTableDefinitionSQLIte(IConnectionManager connection, ObjectNameDescriptor TN) {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;
            var readMetaSql = new SqlTask(SQLiteBasicSql(TN)
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , cid => {; }
            , name => curCol.Name = name.ToString()
            , type => curCol.DataType = type.ToString()
            , notnull => curCol.AllowNulls = (long)notnull == 0 ? true : false
            , dftl_value => curCol.DefaultValue = TryRemoveTrailingSingleQuotes(dftl_value?.ToString())
            , pk => curCol.IsPrimaryKey = (long)pk >= 1 ? true : false
             ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}"
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        static string PostgresBasicSql(bool IsOdbc = false) {
            return $@"
SELECT cols.column_name
     , CASE
           WHEN cols.domain_name IS NOT NULL THEN domain_name
           WHEN cols.data_type = 'character varying' THEN
               CASE
                   WHEN character_maximum_length IS NULL
                       THEN 'varchar'
                   ELSE 'varchar(' || character_maximum_length || ')'
                   END
           WHEN cols.data_type = 'character' THEN 'char(' || character_maximum_length || ')'
           WHEN cols.data_type = 'numeric' THEN
               CASE
                   WHEN numeric_precision IS NULL
                       THEN 'numeric'
                   ELSE 'numeric(' || numeric_precision || ',' || numeric_scale || ')'
                   END
           WHEN LEFT(cols.data_type, 4) = 'time' THEN REPLACE(
                   REPLACE(REPLACE(cols.data_type, 'without time zone', ''), 'with time zone', 'tz'), ' ', '')
           ELSE cols.data_type
    END                                                        AS ""datatype""
     , CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END     AS ""is_nullable""
     , CASE
           WHEN cols.column_default IS NOT NULL AND substring(cols.column_default, 0, 8) = 'nextval' THEN 1
           ELSE 0 END                                          AS ""serial""
     , CASE WHEN tccu.column_name IS NULL THEN 0 ELSE 1 END    AS ""primary_key""
     , cols.column_default
     , cols.collation_name
     , cols.generation_expression
     , tccu.constraint_name                                    AS ""primary_constraint_name""
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN INFORMATION_SCHEMA.TABLES tbl
           ON cols.table_name = tbl.table_name
               AND cols.table_schema = tbl.table_schema
               AND cols.table_catalog = tbl.table_catalog
LEFT JOIN  INFORMATION_SCHEMA.table_constraints tc
           ON cols.table_name = tc.table_name
               AND cols.table_schema = tc.table_schema
               AND cols.table_catalog = tc.table_catalog
               AND tc.constraint_type = 'PRIMARY KEY'
LEFT JOIN  information_schema.constraint_column_usage tccu
           ON cols.table_name = tccu.table_name
               AND cols.table_schema = tccu.table_schema
               AND cols.table_catalog = tccu.table_catalog
               AND tccu.constraint_name = tc.constraint_name
               AND tccu.constraint_schema = tc.constraint_schema
               AND tccu.constraint_catalog = tc.constraint_catalog
               AND cols.column_name = tccu.column_name
WHERE(cols.table_name = { (!IsOdbc ? "@TN1" : "?") }  OR  CONCAT(cols.table_schema, '.', cols.table_name) = { (!IsOdbc ? "@TN2" : "?") })
  AND cols.table_catalog = CURRENT_DATABASE()
ORDER BY cols.ordinal_position
";
        }
        private static TableDefinition ReadBasicTableDefinitionFromPostgres(IConnectionManager connection, ObjectNameDescriptor TN) {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            var readMetaSql = new SqlTask(PostgresBasicSql(connection.IsOdbcOrOleDbConnection)
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , serial => curCol.IsIdentity = (int)serial == 1 ? true : false
            , primary_key => curCol.IsPrimaryKey = (int)primary_key == 1 ? true : false
            , column_default => curCol.DefaultValue = TryRemoveTrailingSingleQuotes(column_default?.ToString().ReplaceIgnoreCase("::character varying", ""))
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
             ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}",
                Parameter = new[] { 
                    new QueryParameter("TN1", "VARCHAR(1000)", TN.UnquotatedFullName),
                    new QueryParameter("TN2", "VARCHAR(1000)", TN.UnquotatedFullName)
                }
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        static string MySqlBasicSql(bool IsOdbc = false) {
            return $@"
SELECT DISTINCT cols.column_name
              , CASE
                    WHEN cols.data_type IN ('varchar', 'char', 'binary')
                        THEN CONCAT(cols.data_type, '(', cols.character_maximum_length, ')')
                    WHEN cols.data_type IN ('decimal') THEN CONCAT(cols.data_type, '(', cols.numeric_precision, ',',
                                                                   cols.numeric_scale, ')')
                    ELSE cols.data_type
    END                                                                                                 AS 'data_type'
              , CASE WHEN cols.is_nullable = 'NO' THEN 0 ELSE 1 END                                     AS 'is_nullable'
              , CASE
                    WHEN cols.extra IS NOT NULL AND cols.extra = 'auto_increment' THEN 1
                    ELSE 0 END                                                                          AS 'auto_increment'
              , CASE WHEN isnull(k.constraint_name) THEN 0 ELSE 1 END                                   AS 'primary_key'
              , CASE WHEN cols.column_default = 'NULL' THEN NULL ELSE cols.column_default END           AS 'column_default'
              , cols.collation_name
              , CASE
                    WHEN cols.generation_expression = '' THEN NULL
                    ELSE cols.generation_expression END                                                 AS 'computed_column'
              , CASE WHEN cols.column_comment = '' THEN NULL ELSE cols.column_comment END               AS 'comment'
              , tc.CONSTRAINT_NAME                                                                      AS 'pk_name'
              , cols.ORDINAL_POSITION                                                                   AS 'ignore'
FROM INFORMATION_SCHEMA.COLUMNS cols
INNER JOIN INFORMATION_SCHEMA.TABLES tbl
           ON cols.table_name = tbl.table_name
               AND cols.table_schema = tbl.table_schema
               AND cols.table_catalog = tbl.table_catalog
LEFT JOIN  INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
           ON cols.table_name = k.table_name
               AND cols.table_schema = k.table_schema
               AND cols.table_catalog = k.table_catalog
               AND cols.column_name = k.column_name
               AND k.constraint_name = 'PRIMARY'
LEFT JOIN  information_schema.TABLE_CONSTRAINTS tc
           ON k.TABLE_SCHEMA = tc.TABLE_SCHEMA
               AND k.TABLE_NAME = tc.TABLE_NAME
               AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
WHERE (cols.table_name = { (!IsOdbc ? "@TN1" : "?") } OR CONCAT(cols.table_catalog, '.', cols.table_name) = { (!IsOdbc ? "@TN2" : "?") })
  AND cols.table_schema = DATABASE()
ORDER BY cols.ORDINAL_POSITION
";
        }

        static string MariaDbBasicSql(bool IsOdbc = false) {
            return $@"
SELECT CAST(a.COLUMN_NAME AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci'     AS 'column_name',
       CAST(a.data_type AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci'       AS 'data_type',
       a.is_nullable,
       a.auto_increment,
       a.primary_key,
       CAST(a.COLUMN_DEFAULT AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci'  AS 'column_default',
       CAST(a.COLLATION_NAME AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci'  AS 'collation_name',
       CAST(a.computed_column AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci' AS 'computed_column',
       CAST(a.comment AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci'         AS 'comment',
       CAST(a.pk_name AS VARCHAR(128)) COLLATE 'utf8mb4_general_ci'         AS 'pk_name',
       a.ignore
FROM (
" + MySqlBasicSql(IsOdbc) + $@"
) a";
        }

        private static TableDefinition ReadTableDefinitionFromMySqlServer(IConnectionManager connection, ObjectNameDescriptor TN) {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            string sql = connection.Compatibility?.ToLower() == "mariadb" ?
                MariaDbBasicSql(connection.IsOdbcOrOleDbConnection) :
                MySqlBasicSql(connection.IsOdbcOrOleDbConnection);

            var readMetaSql = new SqlTask(sql
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
            , auto_increment => curCol.IsIdentity = (int)auto_increment == 1 ? true : false
             , primary_key => curCol.IsPrimaryKey = (int)primary_key == 1 ? true : false
            , column_default => curCol.DefaultValue = TryRemoveTrailingSingleQuotes(column_default?.ToString())
            , collation_name => curCol.Collation = collation_name?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
            , comment => curCol.Comment = comment?.ToString()
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
            , ignore => { }
             ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}",
                Parameter = new[] {
                    new QueryParameter("TN1", "VARCHAR(1000)", TN.UnquotatedFullName),
                    new QueryParameter("TN2", "VARCHAR(1000)", TN.UnquotatedFullName)
                }
            };
            readMetaSql.ExecuteReader();
            return result;
        }


        static string OracleBasicSql(bool IsOdbc = false) {
            return $@"
SELECT cols.COLUMN_NAME
     , CASE
           WHEN cols.DATA_TYPE
               IN ('VARCHAR', 'CHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2', 'NCHAR2', 'VARCHAR2', 'CHAR2')
               THEN cols.DATA_TYPE || '(' || cols.CHAR_LENGTH || ')'

           WHEN cols.DATA_TYPE
               IN ('NUMBER')
               THEN cols.DATA_TYPE || '(' || cols.DATA_LENGTH || ',' ||
                    CASE WHEN cols.DATA_SCALE IS NULL THEN 127 ELSE cols.DATA_SCALE END
               || ')'

           ELSE cols.DATA_TYPE
    END                                                                             AS data_type
     , cols.NULLABLE
     , cols.IDENTITY_COLUMN
     , CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN 'ENABLED' ELSE NULL END            as primary_key
     , cols.DATA_DEFAULT--not working, see restriction above
     , cols.COLLATION
     , cols.DATA_DEFAULT                                                            AS generation_expression--not working, see restriction above
     , CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN cons.CONSTRAINT_NAME ELSE NULL END AS pk_constraint_name
FROM ALL_TAB_COLUMNS cols
LEFT JOIN(
             SELECT concol.table_name,
                    concol.column_name,
                    concol.position,
                    acons.status,
                    acons.owner,
                    acons.constraint_type,
                    acons.CONSTRAINT_NAME
             FROM ALL_CONS_COLUMNS concol
             INNER JOIN ALL_CONSTRAINTS acons
                        ON acons.OWNER = concol.OWNER
                            AND acons.CONSTRAINT_TYPE IN ('P')
                            AND acons.CONSTRAINT_NAME = concol.CONSTRAINT_NAME
                            AND acons.TABLE_NAME = concol.TABLE_NAME
         ) cons
         ON cons.TABLE_NAME = cols.TABLE_NAME
             AND cons.OWNER = cols.OWNER
             --AND cons.position = cols.COLUMN_ID
             AND cons.column_name = cols.COLUMN_NAME
WHERE cols.TABLE_NAME NOT LIKE 'BIN$%'
  AND cols.OWNER NOT IN
      ('SYS', 'SYSMAN', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WKSYS', 'WMSYS', 'XDB', 'ORDPLUGINS',
       'SYSTEM')
  AND (cols.TABLE_NAME = { (!IsOdbc ? ":TN1" : "?") } OR (cols.OWNER || '.' || cols.TABLE_NAME) = { (!IsOdbc ? ":TN2" : "?") })
ORDER BY cols.COLUMN_ID
";
        }
        private static TableDefinition ReadTableDefinitionFromOracle(IConnectionManager connection, ObjectNameDescriptor TN) {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            //Regarding default values: The issue is described partly here
            //https://stackoverflow.com/questions/46991132/how-to-cast-long-to-varchar2-inline/47041776
            //Oracle uses for some system tables still data type "LONG" (should be replaced by LOB)
            //Unfortunately, there is no build-in function to convert LONG into VARCHAR(2) or a LOB/CLOB or similar
            //When running the select, the default value will be displayed, but ADO.NET can't retrieve the value from the database
            //            string sql = $@"
            //SELECT cols.COLUMN_NAME
            //, CASE WHEN cols.DATA_TYPE
            //            IN ('VARCHAR','CHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2', 'NCHAR2', 'VARCHAR2', 'CHAR2' )
            //        THEN cols.DATA_TYPE || '(' || cols.CHAR_LENGTH || ')'
            //	   WHEN cols.DATA_TYPE
            //            IN ('NUMBER')
            //        THEN cols.DATA_TYPE || '(' ||cols.DATA_LENGTH ||',' ||
            //            CASE WHEN cols.DATA_SCALE IS NULL THEN 127 ELSE cols.DATA_SCALE END
            //            || ')'
            //	   ELSE cols.DATA_TYPE
            //    END AS data_type
            //, cols.NULLABLE
            //, cols.IDENTITY_COLUMN
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN 'ENABLED' ELSE NULL END as primary_key
            //, cols.DATA_DEFAULT --not working, see restriction above
            //, cols.COLLATION
            //, cols.DATA_DEFAULT AS generation_expression  --not working, see restriction above
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'U' THEN 'ENABLED' ELSE NULL END as unique_key
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN  cons.CONSTRAINT_NAME ELSE NULL END AS pk_constraint_name
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'U' THEN  cons.CONSTRAINT_NAME ELSE NULL END AS uk_constraint_name
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'R' THEN  cons.r_table_name ELSE NULL END AS fk_table_name
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'R' THEN  cons.r_column_name ELSE NULL END AS fk_column_name
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'R' THEN  cons.CONSTRAINT_NAME ELSE NULL END AS fk_constraint_name
            //FROM ALL_TAB_COLUMNS cols
            //LEFT JOIN (
            //    SELECT a.table_name, a.column_name, a.POSITION
            //           , c.status, c.owner, c.CONSTRAINT_TYPE
            //           , c.CONSTRAINT_NAME
            //           , c_pk.table_name AS r_table_name
            //           , b.column_name AS r_column_name
            //  FROM all_cons_columns a
            //  JOIN all_constraints c ON a.owner = c.owner
            //       AND a.constraint_name = c.constraint_name
            //  LEFT JOIN all_constraints c_pk ON c.r_owner = c_pk.owner
            //       AND c.r_constraint_name = c_pk.constraint_name
            //  LEFT JOIN all_cons_columns b ON C_PK.owner = b.owner
            //       AND  C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION
            // WHERE c.constraint_type IN ('U','P', 'R')
            //    )     cons
            //    ON cons.TABLE_NAME = cols.TABLE_NAME
            //    AND cons.OWNER = cols.OWNER
            //    AND cons.COLUMN_NAME = cols.COLUMN_NAME
            //WHERE
            //    ( cols.TABLE_NAME  = '{TN.UnquotatedFullName}'
            //      OR (cols.OWNER || '.' || cols.TABLE_NAME ) = '{TN.UnquotatedFullName}'
            //    )
            //ORDER BY cols.COLUMN_ID
            //";
            //            string sql = $@" 
            //  SELECT cols.COLUMN_NAME
            //  , CASE WHEN cols.DATA_TYPE
            //              IN('VARCHAR', 'CHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2', 'NCHAR2', 'VARCHAR2', 'CHAR2')
            //          THEN cols.DATA_TYPE || '(' || cols.CHAR_LENGTH || ')'

            //         WHEN cols.DATA_TYPE
            //              IN('NUMBER')
            //          THEN cols.DATA_TYPE || '(' || cols.DATA_LENGTH || ',' ||
            //              CASE WHEN cols.DATA_SCALE IS NULL THEN 127 ELSE cols.DATA_SCALE END
            //              || ')'

            //         ELSE cols.DATA_TYPE
            //      END AS data_type
            //  , cols.NULLABLE
            //  , cols.IDENTITY_COLUMN
            //  , CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN 'ENABLED' ELSE NULL END as primary_key
            //  , cols.DATA_DEFAULT--not working, see restriction above
            // , cols.COLLATION
            //  , cols.DATA_DEFAULT AS generation_expression--not working, see restriction above
            //, CASE WHEN cons.CONSTRAINT_TYPE = 'U' THEN 'ENABLED' ELSE NULL END as unique_key
            //  , CASE WHEN cons.CONSTRAINT_TYPE = 'P' THEN cons.CONSTRAINT_NAME ELSE NULL END AS pk_constraint_name
            // , CASE WHEN cons.CONSTRAINT_TYPE = 'U' THEN cons.CONSTRAINT_NAME ELSE NULL END AS uk_constraint_name
            //  FROM ALL_TAB_COLUMNS cols
            //  LEFT JOIN(
            //    SELECT acols.table_name, acols.column_name, acols.position, acons.status, acons.owner, acons.constraint_type
            //          , acons.CONSTRAINT_NAME
            //    FROM ALL_CONSTRAINTS acons, ALL_CONS_COLUMNS acols
            //    WHERE acons.CONSTRAINT_TYPE IN('U', 'P')
            //    AND acons.CONSTRAINT_NAME = acols.CONSTRAINT_NAME
            //    AND acons.OWNER = acols.OWNER
            //  ) cons
            //  ON cons.TABLE_NAME = cols.TABLE_NAME
            //  AND cons.OWNER = cols.OWNER
            //  --AND cons.position = cols.COLUMN_ID
            //  AND cons.column_name = cols.COLUMN_NAME
            //  WHERE
            //  --cols.TABLE_NAME NOT LIKE 'BIN$%'
            //  --AND cols.OWNER NOT IN('SYS', 'SYSMAN', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WKSYS', 'WMSYS', 'XDB', 'ORDPLUGINS', 'SYSTEM')
            //  --AND
            //      (cols.TABLE_NAME = '{TN.UnquotatedFullName}'
            //        OR(cols.OWNER || '.' || cols.TABLE_NAME) = '{TN.UnquotatedFullName}'
            //      )
            //  ORDER BY cols.COLUMN_ID
            //  ";



            //              LEFT JOIN(
            //      SELECT concol.table_name, concol.column_name, concol.position, acons.status, acons.owner, acons.constraint_type
            //      FROM ALL_CONS_COLUMNS concol
            //      INNER JOIN ALL_CONSTRAINTS acons
            //        ON acons.OWNER = concol.OWNER
            //        AND acons.CONSTRAINT_TYPE IN ('U','P')
            //        AND acons.CONSTRAINT_NAME = concol.CONSTRAINT_NAME
            //        AND acons.TABLE_NAME = concol.TABLE_NAME
            //  WHERE concol.TABLE_NAME NOT LIKE 'BIN$%'
            //  AND concol.OWNER NOT IN('SYS', 'SYSMAN', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'ORDSYS', 'OUTLN', 'WKSYS', 'WMSYS', 'XDB', 'ORDPLUGINS', 'SYSTEM')
            //  AND(concol.TABLE_NAME = '{TN.UnquotatedFullName}'
            //        OR(concol.OWNER || '.' || concol.TABLE_NAME) = '{TN.UnquotatedFullName}'
            //      )
            //  ) cons

            var readMetaSql = new SqlTask(OracleBasicSql(connection.IsOdbcOrOleDbConnection)
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , column_name => curCol.Name = column_name.ToString()
            , data_type => curCol.DataType = data_type.ToString()
            , nullable => curCol.AllowNulls = nullable.ToString() == "Y" ? true : false
            , identity_column => curCol.IsIdentity = identity_column?.ToString() == "YES" ? true : false
             , primary_key => curCol.IsPrimaryKey = primary_key?.ToString() == "ENABLED" ? true : false
            , data_default => curCol.DefaultValue = TryRemoveTrailingSingleQuotes(data_default?.ToString())
            , collation => curCol.Collation = collation?.ToString()
            , generation_expression => curCol.ComputedColumn = generation_expression?.ToString()
            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
             ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}",
                Parameter = new[] {
                    new QueryParameter("TN1", "VARCHAR(1000)", TN.UnquotatedFullName),
                    new QueryParameter("TN2", "VARCHAR(1000)", TN.UnquotatedFullName)
                }
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        static string Db2BasicSql(bool IsOdbc = false) {
            return $@"
SELECT c.COLUMN_NAME                                                  AS column_name
     , CASE
           WHEN c.TYPE_NAME
               IN ('VARCHAR', 'CHARACTER', 'CHAR', 'BINARY', 'VARBINARY', 'CLOB', 'BLOB', 'DBCLOB', 'GRAPHIC',
                   'VARGRAPHIC')
               THEN c.TYPE_NAME || '(' || c.COLUMN_SIZE || ')'
           WHEN c.TYPE_NAME
               IN ('VARCHAR () FOR BIT DATA')
               THEN 'VARBINARY' || '(' || c.COLUMN_SIZE || ')'
           WHEN c.TYPE_NAME
               IN ('CHAR () FOR BIT DATA')
               THEN 'BINARY' || '(' || c.COLUMN_SIZE || ')'
           WHEN c.TYPE_NAME
               IN ('DECIMAL', 'NUMERIC', 'DECFLOAT', 'REAL', 'DOUBLE')
               THEN c.TYPE_NAME || '(' || c.COLUMN_SIZE || ',' || COALESCE(c.DECIMAL_DIGITS, 0) || ')'
           ELSE c.TYPE_NAME
    END                                                               AS data_type
     , CASE WHEN c.NULLABLE = 1 THEN 1 ELSE 0 END                     AS nullable
     , CASE WHEN c.PSEUDO_COLUMN = 2 THEN 1 ELSE 0 END                AS is_identity
     , CASE WHEN pk.PK_NAME IS NOT NULL THEN 1 ELSE 0 END             AS is_primary
     , c.COLUMN_DEF                                                   AS default_value
     --, c.collationname AS collation
     --, c.generated as  generation_expression
     --, c.text as computed_formula
     , c.REMARKS                                                      as description
     , CASE WHEN pk.PK_NAME IS NOT NULL THEN pk.PK_NAME ELSE NULL END AS pk_name
FROM SYSIBM.SQLCOLUMNS c
INNER JOIN SYSIBM.SQLTABLES t
           on
               t.TABLE_SCHEM = c.TABLE_SCHEM and t.TABLE_NAME = c.TABLE_NAME
LEFT JOIN  SYSIBM.SQLPRIMARYKEYS pk
           ON pk.TABLE_NAME = c.TABLE_NAME
               AND pk.TABLE_SCHEM = c.TABLE_SCHEM
               AND pk.COLUMN_NAME = c.COLUMN_NAME
WHERE t.TABLE_TYPE IN ('VIEW', 'TABLE')
  AND (t.TABLE_NAME = { (!IsOdbc ? "@TN1" : "?") }
    OR (TRIM(t.TABLE_SCHEM) || '.' || t.TABLE_NAME = { (!IsOdbc ? "@TN2" : "?") })
    )
ORDER BY c.ORDINAL_POSITION;
";
        }

        private static TableDefinition ReadTableDefinitionFromDb2(IConnectionManager connection, ObjectNameDescriptor TN) {
            TableDefinition result = new TableDefinition(TN.ObjectName);
            TableColumn curCol = null;

            //            string sql = $@" 
            //SELECT c.COLUMN_NAME AS column_name
            //     , CASE WHEN c.TYPE_NAME
            //            IN ('VARCHAR','CHARACTER','CHAR','BINARY','VARBINARY','CLOB','BLOB','DBCLOB','GRAPHIC','VARGRAPHIC' )
            //       THEN c.TYPE_NAME || '(' || c.COLUMN_SIZE || ')'
            //       WHEN c.TYPE_NAME
            //            IN ('VARCHAR () FOR BIT DATA' )
            //       THEN 'VARBINARY' || '(' || c.COLUMN_SIZE || ')'
            //       WHEN c.TYPE_NAME
            //            IN ('CHAR () FOR BIT DATA' )
            //       THEN 'BINARY' || '(' || c.COLUMN_SIZE || ')'
            //	   WHEN c.TYPE_NAME
            //            IN ('DECIMAL','NUMERIC','DECFLOAT','REAL','DOUBLE')
            //       THEN c.TYPE_NAME || '(' || c.COLUMN_SIZE ||',' || COALESCE(c.DECIMAL_DIGITS,0) || ')'
            //	   ELSE c.TYPE_NAME
            //	   END AS data_type
            //     , CASE WHEN c.NULLABLE = 1 THEN 1 ELSE 0 END AS nullable
            //     , CASE WHEN c.PSEUDO_COLUMN = 2 THEN 1 ELSE 0 END AS is_identity
            //     , CASE WHEN pk.PK_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary
            //     , c.COLUMN_DEF AS default_value
            //     --, c.collationname AS collation
            //     --, c.generated as  generation_expression
            //     --, c.text as computed_formula
            //     , CASE WHEN i.NON_UNIQUE = 0 AND pk.PK_NAME IS NULL THEN 1 ELSE 0 END AS is_unique
            //     , c.REMARKS as description
            //     , CASE WHEN pk.PK_NAME IS NOT NULL THEN pk.PK_NAME ELSE NULL END AS pk_name
            //     , CASE WHEN i.NON_UNIQUE = 0 AND pk.PK_NAME IS NULL THEN i.INDEX_NAME ELSE NULL END AS uk_name
            //     , CASE WHEN fk.PKTABLE_NAME IS NOT NULL THEN fk.PKTABLE_NAME ELSE NULL END AS fk_table_name
            //     , CASE WHEN fk.PKCOLUMN_NAME IS NOT NULL THEN fk.PKCOLUMN_NAME ELSE NULL END AS fk_column_name
            //     , CASE WHEN fk.FK_NAME IS NOT NULL THEN fk.FK_NAME ELSE NULL END AS fk_constraint_name
            //FROM SYSIBM.SQLCOLUMNS c
            //INNER JOIN SYSIBM.SQLTABLES t on
            //      t.TABLE_SCHEM = c.TABLE_SCHEM and t.TABLE_NAME = c.TABLE_NAME
            //LEFT JOIN SYSIBM.SQLPRIMARYKEYS pk
            //    ON pk.TABLE_NAME = c.TABLE_NAME
            //    AND pk.TABLE_SCHEM = c.TABLE_SCHEM
            //    AND pk.COLUMN_NAME = c.COLUMN_NAME
            //LEFT JOIN SYSIBM.SQLSTATISTICS i
            //    ON i.TABLE_NAME = c.TABLE_NAME
            //    AND i.TABLE_SCHEM = c.TABLE_SCHEM
            //    AND i.COLUMN_NAME = c.COLUMN_NAME
            //LEFT JOIN SYSIBM.SQLFOREIGNKEYS fk
            //  ON fk.FKTABLE_NAME = c.TABLE_NAME
            //  AND fk.FKTABLE_SCHEM = c.TABLE_SCHEM
            //  AND fk.FKCOLUMN_NAME = c.COLUMN_NAME
            //WHERE t.TABLE_TYPE IN ('VIEW','TABLE')
            //AND ( t.TABLE_NAME = '{TN.UnquotatedFullName}'
            //      OR ( TRIM(t.TABLE_SCHEM) || '.' || t.TABLE_NAME = '{TN.UnquotatedFullName}' )
            //    )
            //ORDER BY c.ORDINAL_POSITION;
            //";

            var readMetaSql = new SqlTask(Db2BasicSql(connection.IsOdbcOrOleDbConnection)
              , () => { curCol = new TableColumn(); }
              , () => { result.Columns.Add(curCol); }
              , column_name => curCol.Name = column_name.ToString()
              , data_type => curCol.DataType = data_type.ToString()
              , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
              , is_identity => curCol.IsIdentity = (int)is_identity == 1 ? true : false
              , is_primary => curCol.IsPrimaryKey = (int)is_primary == 1 ? true : false
              , default_value => curCol.DefaultValue = TryRemoveTrailingSingleQuotes(default_value?.ToString())
              //, collation => curCol.Collation = collation?.ToString()
              //, computed_formula => curCol.ComputedColumn = computed_formula?.ToString()
              , remarks => curCol.Comment = remarks?.ToString()
              , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
               ) {
                DisableLogging = true,
                ConnectionManager = connection,
                TaskName = $"Read column meta data for table {TN.ObjectName}",
                Parameter = new[] {
                    new QueryParameter("TN1", "VARCHAR(1000)", TN.UnquotatedFullName),
                    new QueryParameter("TN2", "VARCHAR(1000)", TN.UnquotatedFullName)
                }
            };
            readMetaSql.ExecuteReader();
            return result;
        }

        //        private static TableDefinition ReadTableDefinitionFromDb2LUW(IConnectionManager connection, ObjectNameDescriptor TN) {
        //            TableDefinition result = new TableDefinition(TN.ObjectName);
        //            TableColumn curCol = null;

        //            string sql_luw = $@" 
        //SELECT c.colname AS column_name
        //     , CASE WHEN c.typename 
        //            IN ('VARCHAR','CHARACTER','BINARY','VARBINARY','CLOB','BLOB','DBCLOB','GRAPHIC','VARGRAPHIC' ) 
        //       THEN c.typename || '(' || c.length || ')'  
        //	   WHEN c.typename 
        //            IN ('DECIMAL','NUMERIC','DECFLOAT','REAL','DOUBLE') 
        //       THEN c.typename || '(' || c.length ||',' || c.scale || ')'
        //	   ELSE c.typename
        //	   END AS data_type 
        //     , CASE WHEN c.nulls = 'Y' THEN 1 ELSE 0 END AS nullable
        //     , CASE WHEN c.identity ='Y' THEN 1 ELSE 0 END AS is_identity
        //     , CASE WHEN i.uniquerule ='P' THEN 1 ELSE 0 END AS is_primary
        //     , c.default AS default_value
        //     , c.collationname AS collation     
        //     --, c.generated as  generation_expression
        //     , c.text as computed_formula
        //    , CASE WHEN i.uniquerule ='U' THEN 1 ELSE 0 END AS is_unique
        //     , c.remarks as description
        //     , CASE WHEN i.uniquerule ='P' THEN i.indname ELSE NULL END AS pk_name
        //     , CASE WHEN i.uniquerule ='U' THEN i.indname ELSE NULL END AS uk_name
        //FROM syscat.columns c
        //INNER JOIN syscat.tables t on 
        //      t.tabschema = c.tabschema and t.tabname = c.tabname
        //LEFT JOIN (
        //    SELECT ix.uniquerule, ix.tabschema, ix.tabname, idxu.colname, ix.indname
        //    FROM syscat.indexes ix
        //    INNER JOIN syscat.indexcoluse idxu
        //    ON idxu.indname = ix.indname
        //    AND idxu.indschema = ix.indschema
        //    ) i
        //ON i.tabschema = c.tabschema 
        //    AND i.tabname = c.tabname
        //    AND i.colname = c.colname     
        //WHERE t.type IN ('V','T')
        //AND ( t.tabname = '{TN.UnquotatedFullName}'
        //      OR ( TRIM(t.tabschema) || '.' || t.tabname = '{TN.UnquotatedFullName}' )
        //    )
        //ORDER BY c.colno;
        //";

        //            var readMetaSql = new SqlTask(sql_luw
        //            , () => { curCol = new TableColumn(); }
        //            , () => { result.Columns.Add(curCol); }
        //            , column_name => curCol.Name = column_name.ToString()
        //            , data_type => curCol.DataType = data_type.ToString()
        //            , is_nullable => curCol.AllowNulls = (int)is_nullable == 1 ? true : false
        //            , is_identity => curCol.IsIdentity = (int)is_identity == 1 ? true : false
        //            , is_primary => curCol.IsPrimaryKey = (int)is_primary == 1 ? true : false
        //            , default_value => curCol.DefaultValue = TryRemoveTrailingSingleQuotes(default_value?.ToString())
        //            , collation => curCol.Collation = collation?.ToString()
        //            , computed_formula => curCol.ComputedColumn = computed_formula?.ToString()
        //            , is_unique => curCol.IsUnique = (int)is_unique == 1 ? true : false
        //            , remarks => curCol.Comment = remarks?.ToString()
        //            , pk_name => result.PrimaryKeyConstraintName = String.IsNullOrWhiteSpace(pk_name?.ToString()) ? result.PrimaryKeyConstraintName : pk_name.ToString()
        //            , uq_name => result.UniqueKeyConstraintName = String.IsNullOrWhiteSpace(uq_name?.ToString()) ? result.UniqueKeyConstraintName : uq_name.ToString()
        //             ) {
        //                DisableLogging = true,
        //                ConnectionManager = connection,
        //                TaskName = $"Read column meta data for table {TN.ObjectName}"
        //            };
        //            readMetaSql.ExecuteReader();

        //            return result;
        //        }

        private static TableDefinition ReadTableDefinitionFromAccess(IConnectionManager connection, ObjectNameDescriptor TN) {
            var connDbObject = connection as IConnectionManagerDbObjects;
            return connDbObject?.ReadTableDefinition(TN);
        }

        protected static string TryRemoveTrailingSingleQuotes(string value) {
            if (!string.IsNullOrEmpty(value) && value.StartsWith("'") && value.EndsWith("'"))
                return value.TrimStart('\'').TrimEnd('\'');
            else
                return value;
        }
    }
}
