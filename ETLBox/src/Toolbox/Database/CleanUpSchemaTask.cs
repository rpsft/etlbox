using ETLBox.Connection;
using ETLBox.Exceptions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Tries to remove all database objects from the given schema(s).
    /// </summary>
    /// <example>
    /// <code>
    /// CleanUpSchemaTask.CleanUp("demo");
    /// </code>
    /// </example>
    public class CleanUpSchemaTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Clean up schema {SchemaName}";
        public void Execute()
        {
            if (ConnectionType != ConnectionManagerType.SqlServer && ConnectionType != ConnectionManagerType.Oracle)
                throw new ETLBoxNotSupportedException("This task is only supported with SqlServer!");
            new SqlTask(this, Sql).ExecuteNonQuery();
        }
        /* Public properties */
        public string SchemaName { get; set; }
        public string Sql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.SqlServer)
                {
                    return $@"
    declare @SchemaName nvarchar(1000) = '{SchemaName}'
    declare @SQL varchar(4000)
	declare @msg varchar(500)

	if OBJECT_ID('tempdb..#dropcode') is not null drop table #dropcode

	create table #dropcode (
		ID int identity(1, 1)
		,SQLStatement varchar(1000)
		)

	-- removes all the foreign keys that reference a PK in the target schema
	select @SQL = 'select
       '' alter table [''+SCHEMA_NAME(fk.schema_id)+''].[''+OBJECT_NAME(fk.parent_object_id)+''] drop constraint ''+ fk.[Name]
    from sys.foreign_keys fk
    join sys.tables t on t.object_id = fk.referenced_object_id
    where t.schema_id = schema_id(''' + @SchemaName + ''')
      and fk.schema_id <> t.schema_id
    order by fk.name desc'

	insert into #dropcode
	exec (@SQL)

	-- drop all default constraints, check constraints and Foreign Keys
	select @SQL = 'select
       '' alter table [''+schema_name(t.schema_id)+''].[''+OBJECT_NAME(fk.parent_object_id)+''] drop constraint ''+ fk.[Name]
    from sys.objects fk
    join sys.tables t on t.object_id = fk.parent_object_id
    where t.schema_id = schema_id(''' + @SchemaName + ''')
     and fk.type IN (''D'', ''C'', ''F'')'

	insert into #dropcode
	exec (@SQL)

	-- drop all other objects in order   
	select @SQL = 'SELECT
      CASE WHEN SO.type=''PK'' THEN '' ALTER TABLE [''+SCHEMA_NAME(SO.schema_id)+''].[''+OBJECT_NAME(SO.parent_object_id)+''] DROP CONSTRAINT [''+ SO.name + '']''
		   WHEN SO.type=''U'' THEN '' DROP TABLE [''+SCHEMA_NAME(SO.schema_id)+''].[''+ SO.[Name] + '']''
           WHEN SO.type=''V'' THEN '' DROP VIEW  [''+SCHEMA_NAME(SO.schema_id)+''].[''+ SO.[Name] + '']''
           WHEN SO.type=''P'' THEN '' DROP PROCEDURE [''+SCHEMA_NAME(SO.schema_id)+''].[''+ SO.[Name] + '']''        		   
           WHEN SO.type=''TR'' THEN ''  DROP TRIGGER [''+SCHEMA_NAME(SO.schema_id)+''].[''+ SO.[Name] + '']''          
		   WHEN SO.type=''SO'' THEN ''  DROP SEQUENCE [''+SCHEMA_NAME(SO.schema_id)+''].[''+ SO.[Name] + '']''
		   WHEN SO.type  IN (''FN'', ''TF'',''IF'',''FS'',''FT'') THEN '' DROP FUNCTION [''+SCHEMA_NAME(SO.schema_id)+''].[''+ SO.[Name] + '']''
       END
       FROM sys.objects SO
       WHERE SO.schema_id = schema_id(''' + @SchemaName + 
		''')
       AND SO.type IN (''PK'', ''FN'', ''TF'', ''TR'', ''V'', ''U'', ''P'',''SO'')
       ORDER BY CASE WHEN type = ''PK'' THEN 1              
              WHEN type = ''TR'' THEN 2
              WHEN type = ''V'' THEN 3
              WHEN type = ''U'' THEN 4
			  WHEN type in (''FN'', ''TF'', ''P'',''IF'',''FS'',''FT'') THEN 5
            ELSE 6
          END'

	insert into #dropcode
	exec (@SQL)

	declare @ID int,@statement varchar(1000)
	declare statement_cursor cursor for select SQLStatement from #dropcode order by ID asc
	open statement_cursor
	fetch statement_cursor into @statement

	while (@@FETCH_STATUS = 0)
	begin
		--print (@statement)
		exec (@statement)
		fetch statement_cursor into @statement
	end

	close statement_cursor
	deallocate statement_cursor	
";
                }
                else if (ConnectionType == ConnectionManagerType.Oracle)
                {
                    string sourceTable = "user_objects";
                    string ownerwhere = $"";
                    if (!string.IsNullOrWhiteSpace(SchemaName))
                    {
                        sourceTable = "all_objects";
                        ownerwhere = $"AND OWNER = UPPER('{SchemaName}')";
                    }
                    return $@"
BEGIN
   FOR cur_rec IN (
                    SELECT object_name, object_type
                    FROM {sourceTable}
                    WHERE object_type IN
                             ('TABLE',
                              'VIEW',
                              'MATERIALIZED VIEW',
                              'PACKAGE',
                              'PROCEDURE',
                              'FUNCTION',
                              'SEQUENCE',
                              'SYNONYM',
                              'PACKAGE BODY'
                             )
                        {ownerwhere}
                    )
   LOOP
      BEGIN
         IF cur_rec.object_type = 'TABLE'
         THEN
            EXECUTE IMMEDIATE 'DROP '
                              || cur_rec.object_type
                              || ' ""'
                              || cur_rec.object_name
                              || '"" CASCADE CONSTRAINTS';
                    ELSE
                       EXECUTE IMMEDIATE 'DROP '
                                         || cur_rec.object_type
                                         || ' ""'
                                         || cur_rec.object_name
                                         || '""';
                    END IF;
                    EXCEPTION
                       WHEN OTHERS
                       THEN
            DBMS_OUTPUT.put_line('FAILED: DROP '
                                  || cur_rec.object_type
                                  || ' ""'
                                  || cur_rec.object_name
                                  || '""'
                                 );
                    END;
                    END LOOP;
                    FOR cur_rec IN(SELECT *
                                    FROM all_synonyms

                                    WHERE table_owner IN(SELECT USER FROM dual))
   LOOP
      BEGIN
         EXECUTE IMMEDIATE 'DROP PUBLIC SYNONYM ' || cur_rec.synonym_name;
                    END;
                    END LOOP;
                    END;
";
                }
                else
                    return string.Empty;
            }
        }

        /* Some constructors */
        public CleanUpSchemaTask()
        {
        }

        public CleanUpSchemaTask(string schemaName) : this()
        {
            SchemaName = schemaName;
        }


        /* Static methods for convenience */
        public static void CleanUp() => new CleanUpSchemaTask().Execute();
        public static void CleanUp(IConnectionManager connectionManager) => new CleanUpSchemaTask() { ConnectionManager = connectionManager }.Execute();
        public static void CleanUp(string schemaName) => new CleanUpSchemaTask(schemaName).Execute();
        public static void CleanUp(IConnectionManager connectionManager, string schemaName) => new CleanUpSchemaTask(schemaName) { ConnectionManager = connectionManager }.Execute();


    }


}
