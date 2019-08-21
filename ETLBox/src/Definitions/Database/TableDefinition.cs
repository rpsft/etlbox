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

        public void CreateTable()=> CreateTableTask.Create(this);

        public void CreateTable(IConnectionManager connectionManager) => CreateTableTask.Create(connectionManager, this);

        internal static TableDefinition GetDefinitionFromTableName(string tableName, IConnectionManager connection)
        {
            TableDefinition result = new TableDefinition(tableName);
            TableColumn curCol = null;
            var readMetaSql = new SqlTask($"Read column meta data for table {tableName}",
$@" 
SELECT cols.name
     , tpes.name
     , cols.is_nullable
     , cols.is_identity
FROM sys.columns cols
INNER JOIN sys.tables tbl
  ON cols.object_id = tbl.object_id
INNER JOIN sys.schemas sc
  ON tbl.schema_id = sc.schema_id
INNER JOIN sys.systypes tpes
  ON tpes.xtype = cols.system_type_id
WHERE (sc.name + '.' + tbl.name ='{tableName}'
       OR  tbl.name = '{tableName}'
      )
  AND tbl.type = 'U'
  AND tpes.name <> 'sysname'"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , name => curCol.Name = name.ToString()
            , colname => curCol.DataType = colname.ToString()
            , is_nullable => curCol.AllowNulls = (bool)is_nullable
            , is_identity => curCol.IsIdentity = (bool)is_identity
             )
            {
                DisableLogging = true,
                DisableExtension = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }


    }
}
