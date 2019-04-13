# Exmample Logging 

## Set up nlog.config

Put a file name nlog.config in the root of your project. This file should look like this:

```xml
<?xml version="1.0" encoding="utf-8"?>
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xsi:schemaLocation="NLog NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <targets>
    <target xsi:type="Database" name="database"
       useTransactions="false" keepConnection="true">
      <commandText>
        insert into etl.Log (LogDate, Level, Stage, Message, TaskType, TaskAction, TaskHash, Source, LoadProcessKey)
        select @LogDate
        , @Level
        , cast(@Stage as nvarchar(20))
        , cast(@Message as nvarchar(4000))
        , cast(@Type as nvarchar(40))
        , @Action
        , @Hash
        , cast(@Logger as nvarchar(20))
        , case when @LoadProcessKey=0 then null else @LoadProcessKey end
      </commandText>
      <parameter name="@LogDate" layout="${date:format=yyyy-MM-ddTHH\:mm\:ss.fff}" />
      <parameter name="@Level" layout="${level}" />
      <parameter name="@Stage" layout="${etllog:LogType=Stage}" />
      <parameter name="@Message" layout="${etllog}" />
      <parameter name="@Type" layout="${etllog:LogType=Type}" />
      <parameter name="@Action" layout="${etllog:LogType=Action}" />
      <parameter name="@Hash" layout="${etllog:LogType=Hash}" />
      <parameter name="@LoadProcessKey" layout="${etllog:LogType=LoadProcessKey}" />
      <parameter name="@Logger" layout="${logger}" />
    </target>
  </targets>
  <rules>
    <logger name="*" minlevel="Debug" writeTo="database" />
  </rules>
</nlog>
```

## Set up the connection, database and log tables

Now you need to create a database and the log tables using the `CreateLogTablesTask`.

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;"));
CreateDatabaseTask.Create("DemoDB");
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB;"));
            
CreateLogTablesTask.CreateLog();
```

This will create two tables (etl.Log and etl.LoadProcess) and some stored procedures to acess these tables. 

## LoadProcess table

This is an example for logging into the log tables

```C#
StartLoadProcessTask.Start("Process 1");
ControlFlow.STAGE = "Staging";
SqlTask.ExecuteNonQuery("some sql", "Select 1 as test");
TransferCompletedForLoadProcessTask.Complete();
ControlFlow.STAGE = "DataVault";

Sequence.Execute("some custom code", () => { });
LogTask.Warn("Some warning!");
EndLoadProcessTask.End("Everything successful");

string jsonLP = GetLoadProcessAsJSONTask.GetJSON();
string jsonLog = GetLogAsJSONTask.GetJSON(1);            
```

`StartLoadProcessTask` and `EndLoadProcessTask` will add information into the etl.LoadProcess table. The other tasks will add records to the etl.Log table.
