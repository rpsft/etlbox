# Logging 

By default, ETLBox uses NLog. NLog already comes with different log targets that be configured either via your app.config or programmatically. 
See the NLog-documentation for a full reference: [https://nlog-project.org/](https://nlog-project.org/)
ETLBox already comes with NLog as dependency - so the necessary packages will be retrieved from nuget automatically through your package manager. 

## Simple Configuration File

In order to use logging, you have to create a `nlog.config` file (with exact this name) and put it into the root folder of your project. 
Make sure that it is copied into your output directory.

A simple and very basic nlog.config could look like this

```xml
<?xml version="1.0" encoding="utf-8"?>
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xsi:schemaLocation="NLog NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <rules>
    <logger name="*" minlevel="Info" writeTo="console" />
  </rules>
  <targets>
    <target name="console" xsi:type="Console" />     
  </targets>
</nlog>
```

After adding a file with this configuration, you will already get some logging output to your console output. 

## Logging into files

Nlog can be easily configured to have your logging output stored in files. Here is an example for a simple configuration that will 
create a Log - folder within your application directory and have a file for every error level provided by Nlog.
(NLog does differentiate each log entry by a log level, which are: Trace, Debug, Info, Warn, Error and Fatal.)

```xml
<?xml version="1.0" encoding="utf-8"?>
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xsi:schemaLocation="NLog NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <targets>
    <target name="file" xsi:type="AsyncWrapper"
            queueLimit="5000"
            overflowAction="Discard">
      <target xsi:type="File" fileName="${basedir}/logs/${level}.txt"
              deleteOldFileOnStartup="true"
              layout="${longdate}|${pad:padding=10:fixedLength=true:inner=${etllog:LogType=STAGE}}|${pad:padding=20:fixedLength=true:inner=${etllog:LogType=Type}}|${pad:padding=5:fixedLength=true:inner=${etllog:LogType=Action}}|${etllog}" />
    </target>
  </targets>
  <rules>
    <logger name="*" minlevel="Debug" writeTo="file" />
  </rules>
</nlog>
```

As you can see, the log output into the file is alreday formatted using particular ETLBox extensions provided by NLog.

### Setting the logging stage

One thing you perhaps want to know in your logging output is the current stage of your data processing. Think you this as a category
for your log output - if things happen during the setup of your database, the Stage could be "SETUP" - if you are currently staging your
data from your sources into your database the stage could be "STAGING".

To set up or modify the current stage, just change the static property `Stage` in the `ControlFlow` class:

```C#
ControlFlow.STAGE = "SETUP"
//some logging
ControlFlow.STAGE = "STAGING"
```

### Disable logging

Perhaps you want some particular tasks or components not to produce any log output, but you don't want to remove the logging completely.
For this case you can use the `DisableLogging` property on every task or component in ETLBox. E.g., if you create a new DBSource, just set 
the property to true:

```C#
DBSource source = new DBSource("TableName") { DisableLogging = true};
```

If you want to disable logging in general for all tasks, you can set the static property `DisableAllLogging` in the ControlFlow class:

```C#
ControlFlow.DisableAllLogging = true;
```

Whenever set to true, no logging output will be produced. When set back to false, logging will be activated again.

## Logging to database

Of course logging to console output or to a file is perhaps not sufficient. If you want to have logging tables in your database, 
ETLBox comes with some additions to the default logging mechanism provided by NLog.

Additionally to the traditional nlog setup where log information can be send to any target by changing the configuration, 
ETLBox comes with a set of Tasks and a recommended nlog configuration. 
This will allow you to have a more advanced logging into your database. 

E.g, you can create log tables and stored procedures useful for logging in SQL with the `CreateLogTablesTask`.
It will basically create two log tables - a table for the "normal" log and a table  to store information about your ETL process. 
Whenever you use a Control Flow or Data Flow task, log information then is written into the log table. 
Additionally, you can use tasks like `StartLoadProcessTask` or `EndLoadProcessTask` which will write information about the current 
ETL process into the ETL process table. 

### Extend the nlog.config

As a first step to have nlog log into your database, you must extend your nlog configuration and add the database as target.
It should then look like this:

```xml
<?xml version="1.0" encoding="utf-8"?>
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xsi:schemaLocation="NLog NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <targets>
    <target name="console" xsi:type="Console" />  
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
    <logger name="*" minlevel="Info" writeTo="console" />
  </rules>
</nlog>
```

### Copy to output directory

Make sure the config file is copied into the output directory where you build executables are dropped. 
Your project configuration file .csproj should contain something like this:

```C#
<Itemgroup>
...
  <None Update="nlog.config">
    <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
  </None>
</Itemgroup>
```

### Create database tables

Now you need some tables in the database to store your log information.
You can use the task `CreateLogTables`. This task will create two tables: 
`etl.LoadProcess` and `etl.Log`.
It will also create some stored procedure to access this tables. This can be useful if you want
to log into these table in your sql code or stored procedures.

**Note**: Don't forget the setup the connection for the control flow.

```C#
CreateLogTablesTask.CreateLog();
```

### LoadProcess table

The table etl.LoadProcess contains information about the ETL processes that you started programmatically with the `StartLoadProcessTask`.
To end or abort a process, you can use the `EndLoadProcessTask` or `AbortLoadProcessTask`. To set the TransferCompletedDate in this table, use
the `TransferCompletedForLoadProcessTask`

This is an example for logging into the load process table.

```C#
StartLoadProcessTask.Start("Process 1 started");
/*..*/
TransferCompletedForLoadProcessTask.Complete();
/*..*/
if (error)
   AbortLoadProcessTask.Abort("This is the abort message");
else 
  EndLoadProcessTask.End("Process 1 ended successfully");
```

### Log Table

The etl.Log table will store all log message generated from any control flow or data flow task. 
You can even use your own LogTask to create your own log message in there.
The following example will create 6 rows in your `etl.Log` table. Every time a Control Flow Tasks starts, 
it will create a log entry with an action 'START'. When it's done with its execution, it will create 
another log entry with action type 'END'

```C#
SqlTask.ExecuteNonQuery("some sql", "Select 1 as test");
Sequence.Execute("some custom code", () => { });
LogTask.Warn("Some warning!");
```

The sql task will produce two log entries - one entry when it started and one entry when it ended its execution.


### Further log tasks

There are some more logging tasks that can be used to manage your log tables.

#### Clean up or remove log table

You can clean up your log with the CleanUpLogTask. 

```C#
CleanUpLogTask.Clean();
```

Or you can remove the log tables and all its procedure from the database. 

```C#
RemoveLogTablesTask.Remove();
```

#### Get log and loadprocess table in JSON

If you want to get the content of the etl.LoadProcess table or etl.Log in JSON-Format, there are two tasks for that:

```
GetLoadProcessAsJSONTask.GetJSON();
GetLogAsJSONTask.GetJSON();
```

#### Custom log messages

If you want to create an entry in the etl.Log table (just one entry, no START/END messages) you can do this using the LogTask. 
Also you can define the nlog level. 

```C#
LogTask.Trace("Some text!");
LogTask.Debug("Some text!");
LogTask.Info("Some text!");
LogTask.Warn("Some text!");
LogTask.Error("Some text!");
LogTask.Fatal("Some text!");
```

## Debugging logging issues

NLog normally behaves very "fault-tolerant". By default, if something is not setup properly or does not work
when NLog tries to log, it just "stops" working without throwing an exception or stopping the execution.
This behavior is very desirable in a production environment, but hard to debug. 

If you need to debug Nlog, you can change the nlog root-element of the nlog.config  into:

```xml
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xsi:schemaLocation="NLog NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      throwConfigExceptions="true"
      autoReload="true"
      internalLogFile="console-example-internal.log"
      internalLogLevel="Info">
```

With this configuration it will raise an exception and also log it into a file.

## ETLBox Logviewer 

**Warning**: The ETLBox Logviewer is still in BETA. Any support to improve this tool is highly appreciated. 

Once you have data in these log tables, you can use the [ETLBox LogViewer](https://github.com/roadrunnerlenny/etlboxlogviewer) to 
easily access and analyze your logs.

<span>
    <img src="https://github.com/roadrunnerlenny/etlbox/raw/master/docs/images/logviewer_screen1.png" width=350 alt="Process Overview of ETLBox LogViewer" />
    <img src="https://github.com/roadrunnerlenny/etlbox/raw/master/docs/images/logviewer_screen2.png" width=350 alt="Process Details of ETLBox LogViewer" />
</span>









