using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ALE.ETLBoxDemo {
    public class ControlFlowTasks {
        public void Start() {
            //Basics
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;"));
            DropDatabaseTask.Drop("DemoDB");
            CreateDatabaseTask.Create("DemoDB");

            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB;"));
            CreateSchemaTask.Create("demo");
            CreateTableTask.Create("demo.table1", new List<TableColumn>() {
                new TableColumn(name:"key",dataType:"int",allowNulls:false,isPrimaryKey:true, isIdentity:true),
                new TableColumn(name:"value", dataType:"nvarchar(100)",allowNulls:true)
            });

            SqlTask.ExecuteNonQuery("Insert data",
               $@"insert into demo.table1 (value) select * from (values ('Ein Text'), ('Noch mehr Text')) as data(v)");

            int count = RowCountTask.Count("demo.table1").Value;
            Debug.WriteLine($"Found {count} entries in demo table!");

            //Truncate:
            //TruncateTableTask.Truncate("demo.table1");

            //Batch processing / Go keyword:

            //SqlTask.ExecuteNonQuery("sql with go keyword", @"insert into demo.table1 (value) select '####';
            //go 2");

            ControlFlow.CurrentDbConnection = new SMOConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB;"));
            SqlTask.ExecuteNonQuery("sql with go keyword", @"insert into demo.table1 (value) select '####';
            go 2");

            AddFileGroupTask.AddFileGroup("FGName", "DemoDB", "200MB", "10MB", isDefaultFileGroup: true);

            CRUDProcedureTask.CreateOrAlter("demo.proc1", "select 1 as test");
        }
    }
}
