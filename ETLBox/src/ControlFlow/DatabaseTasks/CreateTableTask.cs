using ETLBox.Connection;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Creates a table. If the tables exists, this task won't change the table.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateTableTask.Create("demo.table1", new List&lt;TableColumn&gt;() {
    /// new TableColumn(name:"key", dataType:"int", allowNulls:false, isPrimaryKey:true, isIdentity:true),
    ///     new TableColumn(name:"value", dataType:"nvarchar(100)", allowNulls:true)
    /// });
    /// </code>
    /// </example>
    public sealed class CreateTableTask : ControlFlowTask
    {
        #region Shared Properties

        /// <inheritdoc />
        public override string TaskName { get; set; } = $"Create table";

        /// <summary>
        /// The table definition for the table that should be created. Either use the TableDefinition or a combination of
        /// <see cref="TableName"/> and <see cref="TableColumn"/>.
        /// </summary>
        public TableDefinition TableDefinition { get; set; } = new TableDefinition();

        /// <summary>
        /// The formatted table name
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        /// <summary>
        /// The list of columns to create. Either use the <see cref="TableDefinition"/> or a combination of
        /// <see cref="TableName"/> and <see cref="TableColumn"/>.
        /// </summary>
        public IList<TableColumn> Columns {
            get => TableDefinition.Columns;
            set => TableDefinition.Columns = value;
        }

        /// <summary>
        /// The name of the table to create.
        /// </summary>
        public string TableName {
            get => TableDefinition.Name;
            set => TableDefinition.Name = value;
        }

        /// <summary>
        /// A data type converter that is used to remap the current data type names in the TableDefintion
        /// to other, database specific type names. E.g. you can remap that the type VARCHAR(8000) is created as TEXT.
        /// </summary>
        public IDataTypeConverter DataTypeConverter { get; set; } = new DataTypeConverter();

        /// <summary>
        /// When creating the CREATE TABLE sql, ignore the Collation definition that a <see cref="TableColumn"/> potentially has.
        /// </summary>
        public bool IgnoreCollation { get; set; }

        /// <summary>
        /// The sql code that is used to create the table.
        /// </summary>
        public string Sql {
            get {
                return
$@"CREATE TABLE {TN.QuotatedFullName} (
{ColumnsDefinitionSql} {CreatePrimaryKeyConstraint()} {CreateAllUniqueKeyConstraintsSql()} {CreateForeignKeyConstraint()}
)
";
            }
        }

        #endregion

        #region Constructors

        public CreateTableTask() {

        }
        public CreateTableTask(string tableName, List<TableColumn> columns) : this() {
            TableDefinition = new TableDefinition(tableName, columns);
        }

        public CreateTableTask(TableDefinition tableDefinition) : this() {
            TableDefinition = tableDefinition;
        }

        #endregion

        #region Shared implementation 

        internal bool ThrowOnError { get; set; }

        string ColumnsDefinitionSql
            => String.Join("  , " + Environment.NewLine, Columns?.Select(col => CreateTableDefinition(col)));



        private void CheckTableDefinition() {
            if (string.IsNullOrEmpty(TableName))
                throw new ETLBoxException("No table name was provided - can not create or alter the table.");
            if (Columns == null || Columns.Count == 0)
                throw new ETLBoxException("You did not provide any columns for the table - please define at least one table column.");
            if (Columns.Any(col => string.IsNullOrEmpty(col.Name)))
                throw new ETLBoxException("One of the provided columns is either null or empty - can't create table.");
            if (Columns.Any(col => string.IsNullOrEmpty(col.DataType)))
                throw new ETLBoxException("One of the provided columns has a datatype that is either null or empty - can't create table.");
        }

        string CreateTableDefinition(TableColumn col) {
            string dataType = CreateDataTypeSql(col);
            string identitySql = CreateIdentitySql(col);
            string collationSql = CreateCollationSql(col);
            string nullSql = CreateNotNullSql(col);
            string defaultSql = CreateDefaultSql(col);
            string computedColumnSql = CreateComputedColumnSql(col);
            string comment = CreateCommentSql(col);
            if (ConnectionType != ConnectionManagerType.Db2) //Oracle wants the default before nulls, db2 not
                return $@"{QB}{col.Name}{QE} {dataType} {collationSql} {defaultSql} {identitySql} {nullSql} {computedColumnSql} {comment}";
            else
                return $@"{QB}{col.Name}{QE} {dataType} {collationSql} {identitySql} {nullSql} {defaultSql} {computedColumnSql} {comment}";
        }

        private string CreateDataTypeSql(TableColumn col) {
            if (ConnectionType == ConnectionManagerType.SqlServer && col.IsComputed)
                return string.Empty;
            else if (ConnectionType == ConnectionManagerType.Postgres && col.IsIdentity)
                return string.Empty;
            else
                return DataTypeConverter.TryConvertDbDataType(col.DataType, this.ConnectionType);
        }

        private string CreateIdentitySql(TableColumn col) {
            if (col.IsIdentity) {
                if (ConnectionType == ConnectionManagerType.MySql)
                    return "AUTO_INCREMENT";
                else if (ConnectionType == ConnectionManagerType.Postgres)
                    return "SERIAL";
                else if (ConnectionType == ConnectionManagerType.Oracle)
                    return $"GENERATED BY DEFAULT AS IDENTITY START WITH {col.IdentitySeed ?? 1} INCREMENT BY {col.IdentityIncrement ?? 1}";
                else if (ConnectionType == ConnectionManagerType.Db2)
                    return $"GENERATED BY DEFAULT AS IDENTITY (START WITH {col.IdentitySeed ?? 1} INCREMENT BY {col.IdentityIncrement ?? 1})";
                else if (ConnectionType == ConnectionManagerType.SQLite) {
                    if (!col.IsPrimaryKey) //see https://sqlite.org/autoinc.html
                        throw new ArgumentException($"Column {col.Name} is defined as AUTOINCREMENT, but is not a primary key!");
                    else
                        return "PRIMARY KEY AUTOINCREMENT";
                } else
                    return $"IDENTITY({col.IdentitySeed ?? 1},{col.IdentityIncrement ?? 1})";
            } else {
                return string.Empty;
            }
        }

        private string CreateNotNullSql(TableColumn col) {
            string nullSql = string.Empty;
            if (ConnectionType == ConnectionManagerType.Postgres && col.IsIdentity) return string.Empty;
            if (ConnectionType == ConnectionManagerType.Access) return string.Empty;
            if (String.IsNullOrWhiteSpace(col.ComputedColumn))
                nullSql = col.AllowNulls
                            ? "NULL"
                            : "NOT NULL";
            return nullSql;
        }

        private string CreateCollationSql(TableColumn col) {
            if (IgnoreCollation)
                return string.Empty;
            if (ConnectionType == ConnectionManagerType.Db2)
                return string.Empty;
            if (!String.IsNullOrWhiteSpace(col.Collation)) {
                return $"COLLATE {col.Collation}";
            }
            return string.Empty;
        }

        private string CreatePrimaryKeyConstraint(string separator = ",") {
            string result = string.Empty;
            if (Columns.Any(col => col.IsPrimaryKey)) {
                var pkCols = Columns.Where(col => col.IsPrimaryKey);
                if (ConnectionType == ConnectionManagerType.SQLite && pkCols.Any(col => col.IsIdentity))
                    return result;
                string pkConstName = TableDefinition.PrimaryKeyConstraintName ??
                        $"pk_{TN.UnquotatedFullName}_{string.Join("_", pkCols.Select(col => col.Name))}";
                string constraint = $"CONSTRAINT {QB}{pkConstName}{QE}";
                if (ConnectionType == ConnectionManagerType.SQLite) constraint = "";
                if (ConnectionType == ConnectionManagerType.MySql && this.DbConnectionManager.Compatibility?.ToLower() == "mariadb") {
                    constraint = "";
                }                 
                string pkConst = Environment.NewLine + $"{separator} {constraint} PRIMARY KEY ({string.Join(",", pkCols.Select(col => $"{QB}{col.Name}{QE}"))})";
                return pkConst;
            }
            return result;
        }

        private string CreateAllUniqueKeyConstraintsSql(string separator = ",") {
            string result = "";
            if (TableDefinition.UniqueKeyConstraints?.Count > 0) {
                foreach (var constraint in TableDefinition.UniqueKeyConstraints) {
                    result += CreateUniqueKeyConstraintSql(constraint, separator);
                }
            }
            return result;
        }

        private string CreateUniqueKeyConstraintSql(UniqueKeyConstraint constraint,string separator = ",") {
            constraint.Validate();
            string uqConstName = constraint.ConstraintName ??
                        $"uq_{TN.UnquotatedFullName}_{string.Join("_", constraint.ColumnNames)}";
            string constraintText = $"CONSTRAINT {QB}{uqConstName}{QE}";
            if (ConnectionType == ConnectionManagerType.SQLite) constraintText = "";
            string uqSql = Environment.NewLine + $"{separator} {constraintText} UNIQUE ({string.Join(",", GetQuotatedColumnNames(constraint.ColumnNames))})";
            return uqSql;
        }

        private IEnumerable<string> GetQuotatedColumnNames(ICollection<string> columnNames) {
            return columnNames.Select(col => $"{QB}{col}{QE}");
        }

        private string CreateForeignKeyConstraint(string separator = ",") {
            string result = string.Empty;
            if (TableDefinition.ForeignKeyConstraints?.Count > 0) {
                foreach (var constraint in TableDefinition.ForeignKeyConstraints) {
                    result += CreateForeignKeyConstraintSql(constraint, separator); 
                }
            }
            return result;
        }

        private string CreateForeignKeyConstraintSql(ForeignKeyConstraint constraint, string separator) {
            constraint.Validate();
            var refTableName = new ObjectNameDescriptor(constraint.ReferenceTableName, QB, QE);
            string fkConstName = constraint.ConstraintName ??
                $"fk_{TN.UnquotatedFullName}__{refTableName.UnquotatedFullName}_{string.Join("_", constraint.ReferenceColumnNames)}";
            string constraintText = $"CONSTRAINT {QB}{fkConstName}{QE}";
            if (ConnectionType == ConnectionManagerType.SQLite) constraintText = "";
            string fkSql = Environment.NewLine + $"{separator} {constraintText} " + Environment.NewLine +
                $"FOREIGN KEY ({string.Join(",", GetQuotatedColumnNames(constraint.ColumnNames))}) " + Environment.NewLine +
                $"REFERENCES {refTableName.QuotatedFullName}({string.Join(",", GetQuotatedColumnNames(constraint.ReferenceColumnNames))})";
            if (constraint.OnDeleteCascade)
                fkSql += Environment.NewLine + " ON DELETE CASCADE";
            return fkSql;
        }

        private string CreateDefaultSql(TableColumn col) {
            string defaultSql = string.Empty;
            if (!col.IsPrimaryKey)                
                defaultSql = col.DefaultValue != null ? $" DEFAULT {col.DefaultValue}" : string.Empty;
            return defaultSql;
        }

        private string CreateComputedColumnSql(TableColumn col) {
            if (col.IsComputed &&
                (ConnectionType == ConnectionManagerType.SQLite || ConnectionType == ConnectionManagerType.Access))
                throw new NotSupportedException($"ETLBox: Computed columns are not supported for {ConnectionType}");

            if (col.IsComputed) {
                if (ConnectionType == ConnectionManagerType.Postgres || ConnectionType == ConnectionManagerType.MySql)
                    return $"GENERATED ALWAYS AS ({col.ComputedColumn}) STORED";
                else if (ConnectionType == ConnectionManagerType.Db2 || ConnectionType == ConnectionManagerType.Oracle)
                    return $"GENERATED ALWAYS AS ({col.ComputedColumn})";
                else if (ConnectionType == ConnectionManagerType.SqlServer)
                    return $"AS ({col.ComputedColumn}) PERSISTED";
                else
                    return $"AS ({col.ComputedColumn})";
            } else
                return string.Empty;
        }

        private string CreateCommentSql(TableColumn col) {
            if (ConnectionType == ConnectionManagerType.MySql && !string.IsNullOrWhiteSpace(col.Comment))
                return $"COMMENT '{col.Comment}'";
            else
                return string.Empty;
        }

        //string SetQuotesIfString(string value) {
        //    if (!Regex.IsMatch(value, @"^\d+(\.\d+|)$")) {
        //        if (value.StartsWith("'") && value.EndsWith("'"))
        //            return value;
        //        else
        //            return $"'{value}'";
        //    } else
        //        return value;

        //}

        #endregion

        #region Create Implementation 

        /// <summary>
        /// Executes the table creation if the table doesn't exist.
        /// </summary>
        public void CreateIfNotExists() {
            CheckTableDefinition();
            bool tableExists = new IfTableOrViewExistsTask(TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (tableExists && ThrowOnError) throw new ETLBoxException($"Table {TableName} already exists in the database!");
            if (!tableExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /// <summary>
        /// Executes the table creation. Throws an exception if the table exists.
        /// </summary>
        public void Create() {
            ThrowOnError = true;
            CreateIfNotExists();
        }

        /// <summary>
        /// Executes the table creation or execute the corresponding alter statements to adjust the table.
        /// If the table is empty, the new table is always dropped and recreated. 
        /// </summary>
        public void CreateOrAlter() {
            bool tableExists = new IfTableOrViewExistsTask(TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            bool hasRows;
            if (tableExists) {
                hasRows = RowCountTask.HasRows(this.ConnectionManager, TableName);
                if (hasRows)
                    Alter();
                else {
                    DropTableTask.Drop(this.ConnectionManager, TableName);
                    Create();
                }

            } else
                Create();
        }

        #endregion

        #region Alter Implementation 

        public void Alter() {
            ThrowOnError = true;
            AlterIfDifferent();
        }

        /// <summary>
        /// Execute the alter statements to change the table
        /// </summary>
        public void AlterIfDifferent() {
            CheckTableDefinition();
            bool tableExists = new IfTableOrViewExistsTask(TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (!tableExists) throw new ETLBoxException($"Table {TableName} does not exist! Can't alter table.");
            if (tableExists) {
                var statements = CreateAlterSql();
                if (statements.Count > 0) {
                    foreach (string sql in statements)
                        new SqlTask(this, sql).ExecuteNonQuery();
                } else {
                    if (ThrowOnError) throw new ETLBoxException("No changes were detected - can't alter table.");
                }                    //NLogger.Debug($"TableDefinition of new and existing table match. Nothing changed.", TaskType, "RUN", TaskHash, Logging.Logging.STAGE, Logging.Logging.CurrentLoadProcess?.Id);
            }
        }

        TableDefinition ExistingTableDefinition;
        bool WasAlterColumnExecuted;
        List<string> CreateAlterSql() {
            ExistingTableDefinition = TableDefinition.FromTableName(this.DbConnectionManager, TableName);
            return
                CreateAlterStatementsForColumns()
                .Union(CreateAlterStatementsForPrimaryKey())
                .Union(CreateAlterStatementsForUniqueKeys())
                .Union(CreateAlterStatementsForForeignKeys())
                .ToList();
        }

        private List<string> CreateAlterStatementsForColumns() {
            List<string> result = new List<string>();
            foreach (var newcol in TableDefinition.Columns) {
                if (!ColumnIsInDefinition(newcol, ExistingTableDefinition))
                    result.Add(CreateAlterColumnAddSql(newcol));
                else if (ColumnIsInDefinition(newcol, ExistingTableDefinition)) {
                    WasAlterColumnExecuted = false;
                    var existingcol = ExistingTableDefinition.Columns.Where(col => col.Name == newcol.Name).First();

                    if (AreComputedValuesDifferent(newcol, existingcol)) {
                        result.Add(CreateAlterColumnDropSql(existingcol));
                        result.Add(CreateAlterColumnAddSql(newcol));
                        continue;
                    } else if (newcol.IsComputed && !AreComputedValuesDifferent(newcol, existingcol))
                        continue;

                    if ((AreColumnsDataTypesDifferent(newcol, existingcol)
                         || AreCollationsDifferent(newcol, existingcol)
                         )
                        && !WasAlterColumnExecuted)
                        result.Add(CreateAlterColumnSetDataTypeSql(newcol, existingcol));
                    if (newcol.AllowNulls != existingcol.AllowNulls && !WasAlterColumnExecuted)
                        result.Add(CreateAlterColumnSetNullSql(newcol, existingcol));
                    if (AreDefaultValuesDifferent(newcol, existingcol))
                        result.Add(CreateAlterColumnSetDefaultSql(newcol, existingcol));
                }
            }
            foreach (var existingcol in ExistingTableDefinition.Columns) {
                if (!ColumnIsInDefinition(existingcol, TableDefinition))
                    result.Add(CreateAlterColumnDropSql(existingcol));
            }

            return CreateOneOrMultipleStatements(result);
        }

        private string CreateAlterColumnAddSql(TableColumn newcol) {
            return $@"ADD {QB}{newcol.Name}{QE} {CreateDataTypeSql(newcol)} {CreateCollationSql(newcol)} {CreateDefaultSql(newcol)} {CreateNotNullSql(newcol)} {CreateComputedColumnSql(newcol)}";
        }

        private string CreateAlterColumnSetDataTypeSql(TableColumn newcol, TableColumn existingcol) {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxException($"SQLite only supports adding columns via ALTER operations! Can't modify column {newcol.Name}");
            if (ConnectionType == ConnectionManagerType.Postgres)
                return $@"ALTER COLUMN {QB}{newcol.Name}{QE} TYPE {CreateDataTypeSql(newcol)} {CreateCollationSql(newcol)}";
            else if (ConnectionType == ConnectionManagerType.MySql) {
                WasAlterColumnExecuted = true;
                return $@"MODIFY {QB}{newcol.Name}{QE} {CreateDataTypeSql(newcol)}  {CreateCollationSql(newcol)} {CreateNotNullSql(newcol)}";
            } else if (ConnectionType == ConnectionManagerType.Oracle)
                return $@"MODIFY {QB}{newcol.Name}{QE} {CreateDataTypeSql(newcol)} {CreateCollationSql(newcol)}";
            else if (ConnectionType == ConnectionManagerType.Db2)
                return $@"ALTER COLUMN {QB}{newcol.Name}{QE} SET DATA TYPE {CreateDataTypeSql(newcol)} {CreateCollationSql(newcol)}";
            else if (ConnectionType == ConnectionManagerType.SqlServer) {
                WasAlterColumnExecuted = true;
                return $@"ALTER COLUMN {QB}{newcol.Name}{QE} {CreateDataTypeSql(newcol)} {CreateCollationSql(newcol)} {CreateNotNullSql(newcol)}";
            } else
                return string.Empty;
        }

        private string CreateAlterColumnSetNullSql(TableColumn newcol, TableColumn existingcol) {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxException($"SQLite only support adding column via ALTER operations! Can't modify column {newcol.Name}");
            else if (ConnectionType == ConnectionManagerType.MySql) {
                WasAlterColumnExecuted = true;
                return $@"MODIFY {QB}{newcol.Name}{QE} {CreateDataTypeSql(newcol)} {CreateNotNullSql(newcol)}";
            } else if (ConnectionType == ConnectionManagerType.Oracle)
                return $@"MODIFY {QB}{newcol.Name}{QE} {CreateNotNullSql(newcol)}";
            else if (ConnectionType == ConnectionManagerType.Db2 || ConnectionType == ConnectionManagerType.Postgres)
                if (!newcol.AllowNulls)
                    return $@"ALTER COLUMN {QB}{newcol.Name}{QE} SET NOT NULL";
                else
                    return $@"ALTER COLUMN {QB}{newcol.Name}{QE} DROP NOT NULL";
            else if (ConnectionType == ConnectionManagerType.SqlServer) {
                WasAlterColumnExecuted = true;
                return $@"ALTER COLUMN {QB}{newcol.Name}{QE} {CreateDataTypeSql(newcol)} {CreateCollationSql(newcol)} {CreateNotNullSql(newcol)}";
            } else
                return string.Empty;
        }

        private string CreateAlterColumnSetDefaultSql(TableColumn newcol, TableColumn existingcol) {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxException($"SQLite only support adding column via ALTER operations! Can't modify column {newcol.Name}");
            string defaultSql = CreateDefaultSql(newcol);
            if (ConnectionType == ConnectionManagerType.Postgres) {
                if (!string.IsNullOrEmpty(defaultSql))
                    return $"ALTER COLUMN {QB}{newcol.Name}{QE} SET {defaultSql}";
                else
                    return $"ALTER COLUMN {QB}{newcol.Name}{QE} DROP DEFAULT";
            } else if (ConnectionType == ConnectionManagerType.MySql) {
                if (!string.IsNullOrEmpty(defaultSql))
                    return $@"ALTER COLUMN {QB}{newcol.Name}{QE} SET {defaultSql}";
                else
                    return $@"ALTER COLUMN {QB}{newcol.Name}{QE} DROP DEFAULT";
            } else if (ConnectionType == ConnectionManagerType.Oracle) {
                if (!string.IsNullOrEmpty(defaultSql))
                    return $@"MODIFY {QB}{newcol.Name}{QE} {defaultSql}";
                else
                    return $@"MODIFY {QB}{newcol.Name}{QE} DEFAULT NULL";
            } else if (ConnectionType == ConnectionManagerType.Db2) {
                if (!string.IsNullOrEmpty(defaultSql))
                    return $@"ALTER COLUMN {QB}{newcol.Name}{QE} SET {defaultSql}";
                else
                    return $@"ALTER COLUMN {QB}{newcol.Name}{QE} DROP DEFAULT";
            } else if (ConnectionType == ConnectionManagerType.SqlServer) {
                if (!string.IsNullOrEmpty(defaultSql))
                    return $@"ADD {defaultSql} FOR {QB}{newcol.Name}{QE}";
                else {
                    string constraintName = GetDefaultConstraintNameFromSqlServer(newcol.Name);
                    return $@"DROP CONSTRAINT {QB}{constraintName}{QE}";
                }
            } else
                return string.Empty;

        }

        private string CreateAlterColumnDropSql(TableColumn existingcol) {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxException($"SQLite only support adding column via ALTER operations! Can't drop column {existingcol.Name}");
            return $@"DROP COLUMN {QB}{existingcol.Name}{QE}";
        }

        private bool ColumnIsInDefinition(TableColumn column, TableDefinition definition)
            => definition.Columns.Any(defcol => defcol.Name == column.Name);

        private bool AreColumnsDataTypesDifferent(TableColumn newcol, TableColumn existingcol) {
            var existingColType = Helper.DataTypeConverter.GetNETObjectTypeString(existingcol.DataType);
            var newColType = Helper.DataTypeConverter.GetNETObjectTypeString(DataTypeConverter.TryConvertDbDataType(newcol.DataType, this.ConnectionType));
            if (this.ConnectionType == ConnectionManagerType.Oracle) {
                if (newColType.Contains("Int") && existingColType.Contains("Decimal")) return false;
                else
                if (newColType.Contains("Int") && existingColType.Contains("Int"))
                    return existingColType != newColType;
            } else {
                if (newColType.Contains("Int") && existingColType.Contains("Int"))
                    return existingColType != newColType;
            }
            return existingcol.DataType.Trim().ToLower().Replace(" ", "") !=
                    newcol.DataType.Trim().ToLower().Replace(" ", "");
        }

        private bool AreCollationsDifferent(TableColumn colsexisting, TableColumn colsnew) {
            if (IgnoreCollation) return false;
            return false;
        }

        private bool AreDefaultValuesDifferent(TableColumn newcol, TableColumn existingcol) {
            if (string.IsNullOrWhiteSpace(newcol.DefaultValue) && string.IsNullOrWhiteSpace(existingcol.DefaultValue)) return false;
            if ((!string.IsNullOrWhiteSpace(existingcol.DefaultValue) && string.IsNullOrWhiteSpace(newcol.DefaultValue))
                  || (string.IsNullOrWhiteSpace(existingcol.DefaultValue) && !string.IsNullOrWhiteSpace(newcol.DefaultValue))
                  || CompareWithoutDbSpecialChars(existingcol.DefaultValue,newcol.DefaultValue))
                return true;
            else
                return false;
        }

        private bool CompareWithoutDbSpecialChars(string value1, string value2) {            
            value1 = value1.Replace(" ", "").Replace("'","").ToLower();
            value2 = value2.Replace(" ", "").Replace("'","").ToLower();
            if (this.ConnectionType == ConnectionManagerType.SqlServer) {
                value1 = value1.Replace("(", "").Replace(")", "");
                value2 = value2.Replace("(", "").Replace(")", "");                
            }
            return !string.Equals(value1, value2);
        }

        bool AreComputedValuesDifferent(TableColumn newcol, TableColumn existingcol) {
            if (string.IsNullOrWhiteSpace(newcol.ComputedColumn) && string.IsNullOrWhiteSpace(existingcol.ComputedColumn)) return false;
            if ((!string.IsNullOrWhiteSpace(existingcol.ComputedColumn) && string.IsNullOrWhiteSpace(newcol.ComputedColumn))
                  || (string.IsNullOrWhiteSpace(existingcol.ComputedColumn) && !string.IsNullOrWhiteSpace(newcol.ComputedColumn))
                  || existingcol.ComputedColumn != newcol.ComputedColumn)
                return true;
            else
                return false;
        }

        private List<string> CreateOneOrMultipleStatements(List<string> result) {
            //Always create multiple alter statements
            //Postgres and MySql would have a performance gain if statements are combined in one ALTER TABLE
            //but e.g. a data type change don't work with setting a default value in one statement.
            //SqlServer doesn't support this, and Oracle and Db2 are also kind of picky here.
            return result.Select(s => $"ALTER TABLE { TN.QuotatedFullName}" + Environment.NewLine + s).ToList();
        }

        string GetDefaultConstraintNameFromSqlServer(string columnname) {
            string sql = $@"
SELECT defcon.name
FROM sys.tables tbl
INNER JOIN sys.schemas sc
  ON tbl.schema_id = sc.schema_id
INNER join sys.default_constraints defcon 
  ON defcon.parent_object_id = tbl.object_id
INNER JOIN sys.columns c 
  ON c.object_id = tbl.object_id and c.column_id = defcon.parent_column_id
WHERE ( CONCAT (sc.name,'.',tbl.name) ='{TN.UnquotatedFullName}' OR  tbl.name = '{TN.UnquotatedFullName}' ) 
  AND tbl.type IN ('U','V')
  AND c.name = '{columnname}'";
            return (string)SqlTask.ExecuteScalar(this.DbConnectionManager, sql);
        }

        private List<string> CreateAlterStatementsForPrimaryKey() {
            List<string> result = new List<string>();

            var pkcolsexisting = ExistingTableDefinition.Columns.Where(col => col.IsPrimaryKey).ToList();
            var pkcolsnew = TableDefinition.Columns.Where(col => col.IsPrimaryKey).ToList();
            if (AreColumnsDifferent(pkcolsexisting, pkcolsnew)) {
                if (ConnectionType == ConnectionManagerType.SQLite)
                    throw new ETLBoxException("SQLite does not support altering primary keys - drop and recreate the table instead.");
                if (pkcolsexisting.Count > 0)
                    result.Add(DropConstraintSql(ExistingTableDefinition.PrimaryKeyConstraintName));
                if (pkcolsnew.Count > 0)
                    result.Add(AddConstraintSql(CreatePrimaryKeyConstraint("")));
            }
            return result.Select(s => $"ALTER TABLE { TN.QuotatedFullName}" + Environment.NewLine + s).ToList();
        }

        private bool AreColumnsDifferent(List<TableColumn> colsexisting, List<TableColumn> colsnew) {
            if (colsexisting.Count == 0 && colsnew.Count == 0) return false;
            if (colsexisting.Count != colsnew.Count) return true;
            else if (colsexisting.Count == colsnew.Count) {
                bool areequal = true;
                foreach (var existingcol in colsexisting) {
                    if (!(colsnew.Any(newcol => newcol.Name == existingcol.Name)))
                        areequal = false;
                    if (areequal == false) break;
                }
                return areequal;
            }
            return false;
        }

        private string DropConstraintSql(string constraintname) {
            return $@"DROP CONSTRAINT {QB}{constraintname}{QE}";
        }

        private string AddConstraintSql(string constraintsql) {
            return $@"ADD {constraintsql}";
        }

        private List<string> CreateAlterStatementsForUniqueKeys() {
            List<string> result = new List<string>();

            foreach (var existingConstraint in ExistingTableDefinition.UniqueKeyConstraints ?? new List<UniqueKeyConstraint>()) {
                var sameNameConstraint = FindConstraintWithSameName(existingConstraint, TableDefinition.UniqueKeyConstraints);
                if (sameNameConstraint != null && AreConstraintsDifferent(existingConstraint, sameNameConstraint))
                    result.Add(DropConstraintSql(existingConstraint.ConstraintName));
                else if (sameNameConstraint == null)
                    result.Add(DropConstraintSql(existingConstraint.ConstraintName));
            }

            foreach (var newConstraint in TableDefinition.UniqueKeyConstraints ?? new List<UniqueKeyConstraint>()) {
                var sameNameConstraint = FindConstraintWithSameName(newConstraint, ExistingTableDefinition.UniqueKeyConstraints);
                if (sameNameConstraint != null && AreConstraintsDifferent(newConstraint, sameNameConstraint))
                    result.Add(AddConstraintSql(CreateUniqueKeyConstraintSql(newConstraint,"")));
                else if (sameNameConstraint == null)
                    result.Add(AddConstraintSql(CreateUniqueKeyConstraintSql(newConstraint, "")));
            }
            return result.Select(s => $"ALTER TABLE { TN.QuotatedFullName}" + Environment.NewLine + s).ToList();
        }

        private UniqueKeyConstraint FindConstraintWithSameName(Constraint constraint, IEnumerable<Constraint> allConstraints) {
            if (allConstraints == null) return null;
            var sameNameConstraints = allConstraints.Where(constr => constr.ConstraintName == constraint.ConstraintName);
            if (sameNameConstraints.Count() > 1)
                throw new ETLBoxException("More than one constraint with same name found! Constraint names must be unique!");
            else if (sameNameConstraints.Count() == 1)
                return sameNameConstraints.FirstOrDefault() as UniqueKeyConstraint;
            else
                return null;
        }

        private bool AreConstraintsDifferent(Constraint constraint1, Constraint constraint2) {
            var uqcols1 = constraint1.ColumnNames.Select(colName => new TableColumn() { Name = colName }).ToList();
            var uqcols2 = constraint2.ColumnNames.Select(colName => new TableColumn() { Name = colName }).ToList();
            if (AreColumnsDifferent(uqcols1, uqcols2)) {
                if (ConnectionType == ConnectionManagerType.SQLite)
                    throw new ETLBoxException("SQLite does not support altering primary keys - drop and recreate the table instead.");
                return true;
            } else
                return false;
        }

        private List<string> CreateAlterStatementsForForeignKeys() {
            List<string> result = new List<string>();

            foreach (var existingConstraint in ExistingTableDefinition.ForeignKeyConstraints ?? new List<ForeignKeyConstraint>()) {
                var sameNameConstraint = FindConstraintWithSameName(existingConstraint, TableDefinition.ForeignKeyConstraints);
                if (sameNameConstraint != null && AreConstraintsDifferent(existingConstraint, sameNameConstraint))
                    result.Add(DropConstraintSql(existingConstraint.ConstraintName));
                else if (sameNameConstraint == null)
                    result.Add(DropConstraintSql(existingConstraint.ConstraintName));
            }

            foreach (var newConstraint in TableDefinition.ForeignKeyConstraints ?? new List<ForeignKeyConstraint>()) {
                var sameNameConstraint = FindConstraintWithSameName(newConstraint, ExistingTableDefinition.UniqueKeyConstraints);
                if (sameNameConstraint != null && AreConstraintsDifferent(newConstraint, sameNameConstraint))
                    result.Add(AddConstraintSql(CreateForeignKeyConstraintSql(newConstraint, "")));
                else if (sameNameConstraint == null)
                    result.Add(AddConstraintSql(CreateForeignKeyConstraintSql(newConstraint, "")));
            }
            return result.Select(s => $"ALTER TABLE { TN.QuotatedFullName}" + Environment.NewLine + s).ToList();
        }

        #endregion

        #region Static convenience methods

        /// <summary>
        /// Creates a table using a CREATE TABLE statement.
        /// Throws an exception if the table already exists.
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void Create(string tableName, List<TableColumn> columns)
        => new CreateTableTask(tableName, columns).Create();

        /// <summary>
        /// Creates a table using a CREATE TABLE statement.
        /// Throws an exception if the table already exists.
        /// </summary>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void Create(TableDefinition tableDefinition)
            => new CreateTableTask(tableDefinition).Create();

        /// <summary>
        /// Creates a table  using a CREATE TABLE statement.
        /// Throws an exception if the table already exists.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void Create(IConnectionManager connectionManager, string tableName, List<TableColumn> columns)
            => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.Create();

        /// <summary>
        /// Creates a table using a CREATE TABLE statement.
        /// Throws an exception if the table already exists.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void Create(IConnectionManager connectionManager, TableDefinition tableDefinition)
            => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.Create();


        /// <summary>
        /// Creates a table using a CREATE TABLE statement if the table doesn't exist.
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void CreateIfNotExists(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns).CreateIfNotExists();

        /// <summary>
        /// Creates a table using a CREATE TABLE statement if the table doesn't exist.
        /// </summary>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void CreateIfNotExists(TableDefinition tableDefinition) => new CreateTableTask(tableDefinition).CreateIfNotExists();

        /// <summary>
        /// Creates a table using a CREATE TABLE statement if the table doesn't exist.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void CreateIfNotExists(IConnectionManager connectionManager, string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.CreateIfNotExists();

        /// <summary>
        /// Creates a table using a CREATE TABLE statement if the table doesn't exist.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void CreateIfNotExists(IConnectionManager connectionManager, TableDefinition tableDefinition) => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.CreateIfNotExists();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void Alter(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns).Alter();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void Alter(TableDefinition tableDefinition) => new CreateTableTask(tableDefinition).Alter();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void Alter(IConnectionManager connectionManager, string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.Alter();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void Alter(IConnectionManager connectionManager, TableDefinition tableDefinition) => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.Alter();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void AlterIfNeeded(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns).AlterIfDifferent();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void AlterIfNeeded(TableDefinition tableDefinition) => new CreateTableTask(tableDefinition).AlterIfDifferent();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void AlterIfNeeded(IConnectionManager connectionManager, string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.AlterIfDifferent();

        /// <summary>
        /// Alters a table using ALTER TABLE statements.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void AlterIfNeeded(IConnectionManager connectionManager, TableDefinition tableDefinition) => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.AlterIfDifferent();


        /// <summary>
        /// Creates a table if the table doesn't exist or alters a table using ALTER TABLE statements.
        /// If the table does not contain any rows, it will be dropped and created again. 
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void CreateOrAlter(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns).CreateOrAlter();

        /// <summary>
        /// Creates a table if the table doesn't exist or alters a table using ALTER TABLE statements.
        /// If the table does not contain any rows, it will be dropped and created again. 
        /// </summary>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void CreateOrAlter(TableDefinition tableDefinition) => new CreateTableTask(tableDefinition).CreateOrAlter();

        /// <summary>
        /// Creates a table if the table doesn't exist or alters a table using ALTER TABLE statements.
        /// If the table does not contain any rows, it will be dropped and created again. 
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columns of the table</param>
        public static void CreateOrAlter(IConnectionManager connectionManager, string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.CreateOrAlter();

        /// <summary>
        /// Creates a table if the table doesn't exist or alters a table using ALTER TABLE statements.
        /// If the table does not contain any rows, it will be dropped and created again. 
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void CreateOrAlter(IConnectionManager connectionManager, TableDefinition tableDefinition) => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.CreateOrAlter();


        #endregion
    }
}
