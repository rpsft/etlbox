# Change Log

All notable changes to this project will be documented in this file. See [versionize](https://github.com/versionize/versionize) for commit guidelines.

<a name="1.11.2-versionize.2"></a>
## 1.11.2-versionize.2 (2024-08-22)

### Other

* + установил версию 1.11.1
* Add code coverage
* Add Nuget publishing to Gitlab repo
* Add task name to custom destination + task name logging on LinkTo
* Add TConvert generic method type to IDataFlowLinkSource.LinkTo
* Added caching for nuget restored packages
* Added ConnectionManagerType to IConnectionManager interface.
* Added Culture to ITask interface, to be able to read culture for non-db tasks, like CSV
* Added missing QB, QE in SqlOdbcConnectionManager for BulkInsertSql.
* Added parameter support in complete SqlTask (NonQuery, Scalar)
* Added SupportDatabases, SupportProcedures, SupportSchemas, SupportComputedColumns to IConnectionManager. Configured MySql and SQLite. Adjusted different tasks to check for functionality support.
* Added test to improve coverage to get at least the same level as in 1.0.9
* Adding a VoidDestination. Updated docu.
* Adding AfterBatchInsert action
* Adding Aggregation component
* Adding artice about control flow tasks, also moving dataflow tasks to its own namespace.
* Adding async methods and improved sql parsing
* Adding async support (Issue #30)
* Adding attribute handling
* Adding base class for DataFlowDestination
* Adding BaseClass for DropTask
* Adding basic performance tests for LeaveOpen Connections (issue #39)
* Adding basic support for transactions
* Adding batchsize to DbMerge
* Adding bulk insert for Postgres
* Adding ColumnMap and ExcelColumn attributes
* Adding comment for access odbc tests
* Adding CompareColumn attribute for DBMerge (#42)
* Adding composite Keys to table creation and table definition
* Adding Connection Manager and basics
* Adding CrossJoin
* Adding csv source with no header
* Adding CSVDestination
* Adding DataFlowBatchDestination base class
* Adding DataFlowSource Base class
* Adding DataFlowTransformation base class
* Adding DBMerge & Updating docs
* Adding default date serialization - see Issue #34
* Adding DeleteColumn and support for delte in DBMerge
* Adding Delta information as Source for DBMerge
* Adding DeltaTable List to DBMerge
* Adding demo to new ETLBoxDocu project
* Adding docker setup infos
* Adding docu & fixes for JsonSource
* Adding docu for Control Flow and reorganizing articles
* adding documentation
* Adding dynamic for XmlSource
* adding dynamic object support for Sort
* Adding dynamic object to CSVDestination
* Adding dynamic type support for Json and Memory components
* Adding dynamic type to Lookup
* Adding environment var handling
* Adding error buffer to Lookup
* Adding error handling for csv source
* Adding error handling for custom destination
* Adding Error Linking
* Adding error linking for CSVDestination
* Adding error linking for custom source
* Adding ErrorHandling for Json
* Adding example code for multicast and webservice as source
* Adding example for Json and CustomDestination to documentation
* Adding example for reusable data flow
* Adding excel source error handling
* Adding ExcelSource for DataFlows
* Adding exception handling for faulted tasks
* Adding exception if no connection manager exists
* Adding exception tests if table does not exist
* Adding ExceptionHandling to DBSource (rebasing first)
* Adding excpetion handling for DBDestination
* Adding first draft for powershell cmdlet
* Adding full configuration support to have class maps registered in CsvSource (see also Issue #5)
* Adding GCPressure fix #59
* Adding generic CSVSource
* Adding generic task property copy method & fixing cast in dflinker for MergeJoin
* Adding http sources for JsonSource
* Adding HTTPClient for Stream Destinations
* Adding IDataFlowBatchDestination
* Adding IgnoreBlankRows support for ExcelSource - #52
* Adding improved support for special characters in table/schema names
* Adding improved TD support for MySql and sql server
* Adding JsonConverter support for multiple JsonProperty items
* Adding JsonConverter to support JsonPath
* Adding JsonDestination
* Adding JsonSource
* Adding LeaveOpen connection support - issue #39
* Adding legal information
* Adding logging for all DBs
* Adding logging for dataflow tasks
* Adding logging to duplicate example test
* adding Logo 32x32
* Adding memory source/destination tests
* Adding ModifyDBSettings prop to ConnManagers
* Adding more error tolerance to ExpandoJsonConverter
* Adding multicast destination to test (now works with 3 targets)
* Adding namespaces: ETLBox.ControlFlow, ETLBox.Logging, ETLBox.Helper, ETLBox.ConnectionManager
* Adding new feature: All destinations now will ignore nulls
* Adding newly introduced Support-Flags to AccessOdbcConnectionManager
* Adding nlog.config info
* Adding non generic implementation of DBDestination and RowTransformation (based on the sting array generic)
* Adding non generic implementations.
* Adding null handling for custom destination
* Adding nuspec for packaging
* Adding paginated uri
* Adding paramter usage for Odbc connector
* Adding performance test #59
* Adding PKConstraintName support for CreateTableTask #62
* Adding prototype MemoryDestination/Source
* Adding query parameter support for sql task
* Adding release notes
* Adding RowMultiplication transformation
* Adding slide deck
* Adding SQLite support for Dataflow
* Adding SQLite support for reading TableDefinition
* Adding SqlLite ConnectionManager
* Adding StartPostAll
* Adding string arrays as possible type for DBSource
* Adding support for comments in MySql
* Adding support for delta load and ExpandoObjects
* Adding support for dynamic object in ExcelSource
* Adding support for dynamic objects (ExpandoType only)
* Adding support for exists tasks
* Adding support for nested arrays in json files
* Adding support for partly string array
* Adding support for Postgres
* Adding support for reading table definition from access
* Adding support for Views as source in Sql Server
* Adding TableName property to DBSource
* Adding telling exceptions to CreateTableTask
* Adding test
* Adding test for #54
* Adding test for AfterBatchWrite
* Adding test for aggregation with dynamic object
* Adding test for asny completion
* Adding test for connection opening and pooling.
* Adding test for enum types
* Adding test for hash match
* Adding test for logging when executing async
* Adding test for merge with empty source
* adding test for non generic dbsource
* Adding test for writing into json with CustomDestination
* Adding test if DBSource uses a view
* Adding test that copies table based on existing TD for PR #60
* Adding test to check if symbol are excluded for releases
* Adding test to load data from access DB DataSource into Sql Server Data Source
* Adding test to verify GC Pressure #59
* Adding tests for aggregation with attributes
* Adding Tests for Connection Handling
* Adding tests for dynamic object on BlockTransformation
* Adding tests for dynamic support in custom source and destination
* Adding tests for Excel String Array, fixing FlatFile tests
* Adding tests for Issue/PR #4
* Adding tests for performance issue with DBMerge
* Adding tests for ReadLogTask
* Adding tests for special characters in ODBC
* Adding tests for Sql issues
* Adding tests to proof parallel execution of sql task.
* Adding VoidDestination predicate logic when linking - issue #33
* Adding xml destination support for dynamic
* Adding xml documentation
* Adding XmlSource draft
* Addomg log images
* Adjusting Configuration of CSVSource to CSVHelper Configuration
* Adjusting to renaming
* Aggregate with attributes - only one aggregation attribute
* Aggregation now accepts multiple attributes
* All tests debugged on Mac / Docker
* All tests green - initial version of Postgres support
* All tests pass
* Allow more than one beta to be published to nuget.org
* Allow tests to continue run on subsequent failures
* Allowing Arrays for JsonPath Expando Converter
* Almost all tests green for MySql support
* Apply 1 suggestion(s) to 1 file(s)
* Apply 1 suggestion(s) to 1 file(s)
* Apply 1 suggestion(s) to 1 file(s)
* Artifacts publishing fixed
* Avoiding race conditions
* Basic bulk insert support
* BlockTransformation now allows different type for output (See also issue #13)
* Build fixes
* Build fixes
* Build rules to always build on release branch
* changed EUR to $ sign
* Changed title in docu
* Changing default template
* Changing docs to new namespaces
* Changing docu - CurrentDbConnection to DefaultDbConnection
* Changing documentatin style
* Changing documentation
* Changing Post to sendasync.wait
* Changing SetValue to TrySetValue to avoid exceptions
* Changning ChangeAction from string to enum
* CI/CD debug
* CI/CD debug
* CI/CD debug
* cleaning up
* Cleaning up
* Cleaning up
* Code clenaup
* Code clenaup in demo project
* Code formatting & adding Create & Drop method
* Connection String class now also AdoMD compliant.
* ControlFlow tests pass
* CopyTaskProperties method improved
* Correcting interface defintions
* Create _config.yaml
* Create CNAME
* Create CNAME
* Create CNAME
* Create example_basics.md
* Create FUNDING.yml
* Create index.md
* Create pull_request_template.md
* Create style.scss
* Create todo.md
* CreateProcedureTasks now with MySql and Postgres
* Creating ErrorHander class
* Dataflow tests debug
* DBMerge now allows composite keys - issue #42
* DbMerge now supports Expandoobject
* DBSource now supports custom sql
* Debug flat files under gitlab
* Debug local gitlab run
* Debug tests on Kubernetes, somehow Regex works differently (case sensitive)
* Debug unit tests on Apple Silicon M1, most of tests pass
* Debugging CI build
* Default batch size now 1 for Csv and json
* Delete _config.yml
* Delete azure-pipelines.yml
* Delete index.md
* Delete style.scss
* DestinationTableDefinition safety checked and fallback to TableName
* dev/MLRSSL-1127 + стабилизация запуска пакета
* dev/MLRSSL-1127 + стабилизация запуска пакета
* dev/MLRSSL-1127 + стабилизация запуска пакета
* dev/MLRSSL-1127 + удалил лишний логгер из RestTransformation, удалил метод GetLogger
* DFStreamDestination base class added
* Doc generation is moved to doxygen
* Docs updated - now with Google Analytics.
* Documentation changed
* Documentation improved
* Documentation moved into docu project
* Documentation update
* Documentation update
* Documentation updated - new link to video added.
* Eliminated Sonarcube for now
* EOL LF -> CRLF for all .cs and .csv files
* Error Linking now accepts multiple sources
* Example code for Issue #5 - Duplicate check with RowTransformation and BlockTransformation
* Example dataflow added
* Example test added. Updating package version.
* Example test for Issue #6 (consuming a webservice with a custom source)
* Excel source now ignore blank rows
* ExcelSource now takes header row into account
* Expose destination completion tasks so several can be awaited at once
* Extending DBSource - columns are now mapped to property names. DBDestination now avoids Identity columns when matching prop Names
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: fix running separated test projects
* feature/MLRSSL-1127: fixed serialization tests project
* feature/MLRSSL-1127: fixt a pack of warnings, cklickhouse moved to target netstandard2.1, fixed a test of kafka
* feature/MLRSSL-1127: konfigure kafka on gitlab-cy
* feature/MLRSSL-1127: redesign test s of kafka from testcontainers to external service
* feature/MLRSSL-1127: remove debug running tests
* feature/MLRSSL-1127: remove logging a progress on dbdestination
* feature/MLRSSL-1127: remove unused file
* feature/MLRSSL-1127: return a single dotnet test and fix a progress
* feature/MLRSSL-1127: separated dotnet test for all projects
* feature/MLRSSL-1127: вернул запрет на артифакты если упали тесты
* feature/MLRSSL-1127: добавляю тесты в CI gitlab на мерж-реквесте
* feature/MLRSSL-1161: fix kafka test
* feature/MLRSSL-1161: fix warnings
* Finalizing Aggregation
* Finalizing CrossJoin
* Finalizing lookup
* Finalizing tests, adding last CF task for next version
* Finalizing transaction support
* First dataflow with Bulk Insert
* First draft for fluent LinkTo implementation - Issue #33
* First draft implementation on RowTransformation for automatic exception handling - #41
* First implementation of ODBC support and bulk Insert with ODBC
* First implementation of XmlDestination
* First prototype
* First prototype
* First protoype for DbMerge with Dynamic
* Fix "File not found" in unit tests
* Fix build
* Fix for Bulk Insert, improving SQLite support
* Fix for duplicated CI builds on merge requests
* Fix for duplicated CI builds on merge requests
* Fix for https://sonarqube.rapidsoft.ru/project/issues?id=open-source_etlbox_AX2rV2MSOqMd9uKUCs_O&open=AX2y9dSNqZnYS8uXZe1l&resolved=false&types=BUG
* Fix for Issue #5 - for the DBDestination, the name of the properties were not matched with the column names of the destination table.
* Fix one more "File not found"
* Fix publish rules
* Fix version auto-numbering
* Fix version auto-numbering
* Fix version auto-numbering
* fix:RSSL-10003 - remove vulnurabilities from dependencies
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* Fixed bug :setting the value of the Enum property.
* Fixed bug :setting the value of the Enum property.
* Fixed bug in Uri output & fixing tests
* Fixed bug with ConnectionManager in DBSource.  Using a DBSource as non-generic class now works flawless.
* Fixed issue with data load into database in case of flat file.
* Fixed naming issue of constructor for Issue #17
* Fixed package Id to avoid conflict with EtlBox 2.x, 3.x
* Fixed version counter
* Fixes for single-processor execution under docker
* Fixes for SQL Server data types:
* Fixes to nuget.org publish procedure
* Fixing < issues
* Fixing ArgumentNulLException #54
* Fixing batch size bug when creating TableDef in DbDestination
* Fixing bug for only one ExcelColumn
* Fixing bug for only one ExcelColumn
* Fixing bug in DBMerge with higher amount of data
* Fixing bug in MemoryDestination when data exceeds batch size
* Fixing bug in multicast with readonly properties
* Fixing bug when reading TableDefinition in Postgres #48
* Fixing bug when using default db connection in ControlFlow object
* Fixing bug when using more excel columns than props
* Fixing bug with error when table name not in default schema
* Fixing connection issues with Access Db
* Fixing ControlFlow Tests
* Fixing database connector tests
* Fixing Issue #17 - sql without tabledefinition will work now
* Fixing issue when reading PK constraints on table with Index
* Fixing issue with different property sequences and ExpandoObject in DbDestination
* Fixing issues with ExpandoJsonConverter and CreateTableTask
* Fixing perf test for MemDest
* Fixing performance issue in DbMerge #54
* Fixing regex for ObjectNameDescriptor
* Fixing TableDefinition sql #43
* Fixing test error for null values in dynamic objects
* Fixing test for Bulk Insert with AccessOdbcConnectionManager
* Fixing tests - repairing changes to DbDestination
* fixing tests for DbMerge
* Fixing timestamp issue for Postgres #46
* Fixing typos
* Fixing xml comment issues
* GCPressure fix can be disabled now
* Gitlab test setup
* GitLab tests and collector
* Google analytics code added.
* Having task type only in base class
* Ignore null values of cross join output
* Implementation of DBMerge
* Implemented own completion logic
* Improved CF tasks for Access
* Improved connection handling for DbSource
* Improved error message when no column names could be parsed from DbSource #57
* Improved logging for Dataflow tasks.
* Improving constructor code DbMerge
* Improving ControlFlow SQLite support
* Improving DBMerge - adding IdColumn attribute (#42)
* Improving docu
* Improving docu
* Improving docu
* Improving docu
* Improving docu
* Improving docu for complex example
* Improving Exception handling
* Improving excpetion handling for Lookup and renaming CSVSource to CsvSource
* Improving lookup - testing shortcut for output equals input type
* Improving Performance Tests
* Improving readme
* Improving readme
* Improving SQLite support, improving BulkInserts
* Improving StartLoadProcessTask
* Improving tests
* Initial commit
* Initial commit
* Introducing RowDuplication
* Issue #18 Adding OnCompletion for CustomDestination
* Issue #5 - completing example test.
* Issue#22 Handle empty cells in ExcelSource
* json source with new base class
* JsonWriter отлажен с отправкой в HTTP POST
* Line endings modified for JSON
* Link to imprint repaired
* Link to imprint repaired again
* Logging refactored
* Making img path relative
* MemorySource now allows IEnumerable
* Merge branch 'bencassie-feature/linkto-type-convert' into dev
* Merge branch 'bencassie-feature/task-names' into dev
* Merge branch 'bugfix/RSSL-10003' into 'develop'
* Merge branch 'bugfix/RSSL-10005' into 'develop'
* Merge branch 'bugfix/RSSL-10023' into 'develop'
* Merge branch 'bugfix/RSSL-10023' into 'develop'
* Merge branch 'bugfix/RSSL-10023' of https://git.rapidsoft.ru/open-source/etlbox into bugfix/RSSL-10023
* Merge branch 'bugfix/RSSL-9416-3' into 'develop'
* Merge branch 'bugfix/RSSL-9416-egors-pk' into 'bugfix/RSSL-9416-egors'
* Merge branch 'bugfix/RSSL-9416-egors' into 'develop'
* Merge branch 'bugfix/RSSL-9416' into 'develop'
* Merge branch 'bugfix/RSSL-9416' into 'develop'
* Merge branch 'bugfix/RSSL-9416' into 'develop'
* Merge branch 'bugfix/RSSL-9851' into 'develop'
* Merge branch 'bugfix/RSSL-9862' into 'develop'
* Merge branch 'bugfix/set_version' into 'develop'
* Merge branch 'Completion_Complex_Graphs' into dev
* Merge branch 'dev'
* Merge branch 'dev'
* Merge branch 'dev'
* Merge branch 'dev' into dev
* Merge branch 'dev' of https://github.com/roadrunnerlenny/etlbox into dev
* Merge branch 'dev/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1137-refactor' into 'feature/MLRSSL-1137'
* Merge branch 'dev/MLRSSL-1153' into 'develop'
* Merge branch 'dev/MLRSSL-1161-1' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-2' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-3' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-merge' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk-logs' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk-logs' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161-egors'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161-egors'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161' into 'feature/MLRSSL-1161'
* Merge branch 'dev/RSSL-8664' into 'master'
* Merge branch 'dev/RSSL-8664' into 'master'
* Merge branch 'dev/RSSL-9421-3' into 'develop'
* Merge branch 'dev/RSSL-9421-serializer' into 'develop'
* Merge branch 'dev/RSSL-9421-tests' into 'develop'
* Merge branch 'dev/RSSL-9851' into 'feature/MLRSSL-1255'
* Merge branch 'develop' into dev/RSSL-9421-tests
* Merge branch 'develop' into feature/MLRSSL-1255
* Merge branch 'feature/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1127' into 'pass_tests'
* Merge branch 'feature/MLRSSL-1137' into 'dev/MLRSSL-1137-refactor'
* Merge branch 'feature/MLRSSL-1137' into 'develop'
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1161-egors' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1161' into 'develop'
* Merge branch 'feature/MLRSSL-1161' into 'feature/MLRSSL-1127'
* Merge branch 'feature/MLRSSL-1161' into 'pass_tests'
* Merge branch 'feature/MLRSSL-1161' into dev/MLRSSL-1161-2
* Merge branch 'feature/MLRSSL-1161' into feature/MLRSSL-1161-egors
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1255' into 'develop'
* Merge branch 'feature/RSSL-9862' into 'develop'
* Merge branch 'ForkImprovements' into dev
* Merge branch 'hotfix/MLRSSL-1002' into 'master'
* Merge branch 'hotfix/package-id' into 'master'
* Merge branch 'hotfix/package-id' into 'master'
* Merge branch 'hotfix/RSSL-8664' into 'master'
* Merge branch 'hotfix/SYSOPS-1035' into 'master'
* Merge branch 'Improving_Lookup' into dev
* Merge branch 'Issue1' into release
* Merge branch 'master' into dev
* Merge branch 'master' into develop
* Merge branch 'master' into develop
* Merge branch 'master' into release/1.10
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'pass_tests' into feature/MLRSSL-1161
* Merge branch 'RefactoringTests' into dev
* Merge branch 'release/1.10' into 'develop'
* Merge branch 'release/1.10' into 'master'
* Merge branch 'release/1.8.10' into 'master'
* Merge branch 'release/1.9.1' into 'master'
* Merge branch 'versionize' into 'develop'
* Merge of changes on getting started
* Merge pull request #1 from mukundnc/mukundnc-patch-1
* Merge pull request #20 from bruce-dunwiddie/dev
* Merge pull request #21 from roadrunnerlenny/dev
* Merge pull request #38 from bencassie/fix/dbdestination-taskname-null-check
* Merge pull request #4 from mukundnc/master
* Merge pull request #60 from shokurov/dev
* Merge pull request #63 from vladislav-smr/dev
* Merge pull request #65 from vladislav-smr/dev
* Merge pull request #66 from vladislav-smr/dev
* Merge pull request #67 from SipanOhanyan/patch-1
* Merge pull request #68 from SipanOhanyan/patch-3
* Merge pull request #69 from SipanOhanyan/patch-4
* Merge remote-tracking branch 'origin/dev/MLRSSL-1161-merge' into dev/MLRSSL-1161-merge
* Merge remote-tracking branch 'origin/develop' into dev/MLRSSL-1153
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1154
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1154
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1154
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1161
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161-egors' into feature/MLRSSL-1161-egors
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into dev/MLRSSL-1161-3
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into dev/MLRSSL-1161-pk-logs
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into feature/MLRSSL-1161-egors
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1255' into dev/RSSL-9851
* Merge remote-tracking branch 'origin/gitlab-build' into gitlab-build
* Merge remote-tracking branch 'origin/master' into hotfix/RSSL-8664
* Migrate tests to net5.0
* Migrate to net6.0
* Minor code style changes
* Minor docu updates
* Minor fixes to xml comments
* MLRSSL-1002: Fixed create table task on PostgreSql
* MLRSSL-1002: Fixed create table task on PostgreSql - bigint identity changed from serial to generated by default as identity
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1128 Доработать ETLBox для репликации данных из Clientrix и БДК
* MLRSSL-1134 PoC. Разработать шаг ETL для DataFlow через EtlBox
* MLRSSL-1134 PoC. Разработать шаг ETL для DataFlow через EtlBox
* MLRSSL-1135 PoC for Kafka DataflowSource
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fixed start container for kafka
* MLRSSL-1137 + добавил логирование Url и Body rest-запроса
* MLRSSL-1137 + добавил логирование Url и Body rest-запроса
* MLRSSL-1137 + добавил реализацию агрегации через настройку мэппингов
* MLRSSL-1137 + отменил изменения JsonTransformation
* MLRSSL-1137 + починил JsonTransformation
* MLRSSL-1137 + починил JsonTransformation, добавил режим обработки не найденных полей в ScriptedRowTransformation
* MLRSSL-1137 + правка по комментам к мержу
* MLRSSL-1137 + правки по комментам к мержу
* MLRSSL-1137 + удалил вызов logger.BeginScope
* MLRSSL-1137 Refactored Mappings implementation for Aggregation
* MLRSSL-1153 A transformation step has been developed based on Microsoft.CSharpScript
* MLRSSL-1153 Added nested dynamics to the test
* MLRSSL-1153 Added type check guard for anonymous types
* MLRSSL-1153 Improved error reporting, added error line and pointer to the compilation error message
* MLRSSL-1154 Fixed namespaces back
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + добавил DbTransformation
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + добавил DbTransformation
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + доработал создание пакетов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + доработал создание пакетов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + доработал создание пакетов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + закомментил SQLite
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поменял таргеты на netstandard2.0
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил namespaces
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил namespaces
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил namespaces
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил тест
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поубирал часть варнингов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + пофиксил десериализацию
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + удалил перенесенные тесты
* MLRSSL-1161 + add a IList deserialization
* MLRSSL-1161 + added test
* MLRSSL-1161 + added test
* MLRSSL-1161 + added test
* MLRSSL-1161 + fix a deserialization
* MLRSSL-1161 + fix a JsonTransformation
* MLRSSL-1161 + fix a JsonTransformation
* MLRSSL-1161 + fix a JsonTransformation, add test
* MLRSSL-1161 + fix loading libraries into memory
* MLRSSL-1161 + remove unused namespace
* MLRSSL-1161 + rename test
* MLRSSL-1161 + renamed test
* MLRSSL-1161 + return error logging logic
* MLRSSL-1161 + починил логирование в Etl, доработал RestTransformation
* MLRSSL-1161 + починил тесты
* MLRSSL-1161 + починил тесты
* MLRSSL-1161 Added TreatWarningsAsErrors for all builds
* MLRSSL-1161 Added TreatWarningsAsErrors for CI builds
* MLRSSL-1161 Added TreatWarningsAsErrors for CI builds
* MLRSSL-1161 EtlBox: добавить возможность безопасно прервать процесс DataFlow
* MLRSSL-1161 EtlBox: добавить возможность безопасно прервать процесс DataFlow
* MLRSSL-1161 Fixed all tests, except TestDatabaseConnectors
* MLRSSL-1161 Fixed Kafka tests running within single client group
* MLRSSL-1161 Fixing remaining tests
* MLRSSL-1161 Refactored warnings, moved REST tests to a separate project
* MLRSSL-1161 Refactoring CI CD
* MLRSSL-1161 Refactoring CI CD
* MLRSSL-1161 Refactoring CI CD
* MLRSSL-1255 + PoC SqlQueryTransformation
* More styles need to be overwritten
* Most parts refactored
* Mostly converted to net core. Still issues with nlog that need to be fixed.
* Mostly logging refactored
* Moved ETLBoxCmdlets into its own repo
* Moving _site to /docs
* Moving config helper class into test shared library
* Moving connection manager tests into separate Test Project
* Moving CsvSource to new base class
* Moving ETLBoxDemo into it's own repo
* nlog now properly setup, all tests green.
* Non generic implementation for lookup
* Now full support for SQLite
* Now returning column name as opposed to possibly escaped column name from raw text.
* Odbc and AccessOdbc Connection Manager added, including tests.
* Odbc support - adding Odbc connection string and some tests. Odbc bulk insert replacement is not available yet.
* One more fix to changed SQLite datetime formats
* Overhauling complete logging
* Overwrite of .navbar-inverse now with !important rule
* Overwriting navbar-inverse - minor change to docs
* Performance tests, adjusting ReleaseGCPressure for faster execution speed
* Pre-release 1.7.5
* Preparation release 1.4.1
* Preparing 1.6.5, improving MemoryDestination
* Preparing 1.7.1
* Preparing DataFlowLinker
* Preparing for Odbc Issue
* Preparing for release
* Preparing v. 1.6.2
* Preparing v1.7.4, changing back to .NET Standard 2.0
* Preparing v1.7.7
* Preparing v1.8.1
* Preparing v1.8.2, updating docu
* Preparing v1.8.7
* Preparing version 1.6.0
* Preparing version 1.6.3
* Preparing version 1.8.4
* Preparing version 1.8.6
* Preparing version 1.8.8
* Preparing version 1.8.8 alpha
* Pulling up test coverage
* Pulling up test coverage
* Recreating documents with new namespace help
* Refactoring base classes dataflow
* Refactoring constructor logic
* Refactoring ControlFlow and Logging Tests
* Refactoring DataFlow Tests
* Refactoring DBSource + adding new test for multiple sources into one destination
* Refactoring DF Destination
* Refactoring LinkTo into composite class
* Refactoring RowDuplication/RowMultiplication by using TransformationManyBlock
* Refactoring TypeInfo
* Refactorting and improving ConnectionStrings
* Refining ControlFlowTasks for Postgres
* Release 1.4.0
* Release notes - added DBSource
* Release notes improved
* Releasing new docu to website
* Releasing Version 1.5.0
* Releasing version 1.7.5
* Remove unnecessary logging
* Removed ConnectionManagerSpecifics class.
* Removed unnecessary test
* Removed unneeded props
* Removing a !NOTE md
* Removing CleanUpLogTask
* Removing clutter
* Removing deprecated sql extension code
* Removing Execute method from GenericTask
* Removing FileConnectionManager & smaller renamings
* Removing FileGroupTask
* Removing IDbConnectionManager
* Removing old test project
* Removing supefluous lines
* Removing symbols file for Release version
* removing unnecessary ref
* removing unnecessary ref keyword
* Removing unnecessary usage of Command in DbTask
* Removing useless IsConnectionOpen-Check
* Rename _config.yaml to _config.yml
* Renamed "Delete" to "Drop" in DropDatabaseTask
* Renaming ConnectionString to SqlConenctionString, adjusting tests to name changes
* Renaming CSV/DB to Csv/Db in Tests and docu (Search & Replace)
* Renaming CSVSource/CSVDest to CsvSource/CsvDest
* Renaming DBSource & DBDestination to DbSource and DbDestination
* Renaming ExecuteAsync To PostAll
* Renaming Issue3 testcases
* renaming ReadTopX to Limit
* Renaming TableNameDescriptor, improving queries with new ObjectNameDescriptor
* Renaming Test
* Renaming test directories
* Renaming test projects, continuing with refactor DF tests
* Renaming tests for former non generic
* Renaming to project standard. Step 2
* Renaming to project standard. Step 2
* Repairing API documentation
* Repairing demo
* Repairing tests - avoiding database locked error and skipping not maintained tests
* Repairing toc.yml
* Replacing mdb with accdb files
* Resetting batch size to 1000 for json, csv and memory dest
* Resolving Access ODBC connections issues in tests
* Resolving GC pressure
* Revert "MLRSSL-1137 + добавил логирование Url и Body rest-запроса"
* Rewriting basic example
* RSSL-8664 Added coverage analysis
* RSSL-8664 Added publishing to nuget.org (only -beta and release versions)
* RSSL-8664 Additional clean up
* RSSL-8664 Bumped all dependencies to current versions
* RSSL-8664 bumped version in master
* RSSL-8664 CI test debug
* RSSL-8664 Code Cleanup
* RSSL-8664 Coverage report regex fixed
* RSSL-8664 CsvHelper updated to v.30
* RSSL-8664 Debugging CI test_job
* RSSL-8664 Debugging CI test_job
* RSSL-8664 Debugging one more unit test, where source and destination used the same connection
* RSSL-8664 Debugging parallel and repetitive test runs
* RSSL-8664 Fix for nuget publishing
* RSSL-8664 Fix nuget.org publishing
* RSSL-8664 Fixed nuget.org publishing to manual
* RSSL-8664 Fixing tests, refactoring, eliminate compiler warnings
* RSSL-8664 MySQL corner case unit test fixed
* RSSL-8664 Packaging and docs
* RSSL-8664 Readme.md updated with new badges
* RSSL-8664 Removed comments on local ms sql container
* RSSL-8664 Rename CSV -> Csv, step 1
* RSSL-8664 Rename CSV -> Csv, step 2
* RSSL-8664 Rename CSV -> Csv, step 3
* RSSL-8664 Unit test debug
* RSSL-8664 Updated README.md
* RSSL-8664 Настройка публикации NUget
* RSSL-8664 Отладка публикации на nuget.org
* RSSL-9416 Add a test for json serialization of property
* RSSL-9416 Added reference extensibility to scripting transformation
* RSSL-9416 CLONE - PoC: Добавить респондента в опросы, и сформировать для каждого уникальную ссылку
* RSSL-9416 Fix formatting
* RSSL-9416 Fix test name
* RSSL-9416: Adapt ScriptedTransformation to receive an assembly relative path
* RSSL-9416: Adapt ScriptedTransformation to receive an assembly relative path
* RSSL-9416: Add a default constructor to DbMerge, add test
* RSSL-9416: Add an ability to Process Newid and JsonSerialization to ScriptedRowTransformation
* RSSL-9416: Add check parameters for null to RestMethodInternalAsync
* RSSL-9416: Fix and error on desereialize non-generic interfaces
* RSSL-9416: Fix serialization
* RSSL-9416: Fix serialization, add test
* RSSL-9416: Fixed DbMerge
* RSSL-9416: Fixed DbMerge & DbTransformation deserialization
* RSSL-9416: Fixed IDataFlowSource (add ILinkErrorSource interface inheritance)
* RSSL-9416: Simplified tests
* RSSL-9416: Simplified tests
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421: Fixed HttpStatusCodeException
* RSSL-9421: Fixed RestTransformation & DataFlowXmlReader, improved tests
* RSSL-9421: Implemented DataFlow serialization extensions (read from xml) & tests
* RSSL-9421: Implemented RestTransformationTests
* RSSL-9421: Implemented RestTransformationTests
* RSSL-9421: Implemented RestTransformationTests
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9851 - add a comments and rename a test
* RSSL-9851 - add a table definition for complex custom select
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9862: Add new tests for DataFlowXmlReader
* RSSL-9862: Fixed DataFlowXmlReader (GetValue)
* RSSL-9862: Fixed DataFlowXmlReader (GetValue) & TypeExtensions; Add new tests
* RSSL-9862: Fixed DataFlowXmlReader extensions
* RSSL-9862: Fixed DataFlowXmlReader tests
* RSSL-9862: Fixed DataFlowXmlReader tests
* RSSL-9862: Fixed tests for DataFlowXmlReader
* RSSL-9862: Fixed tests for DataFlowXmlReader
* RSSL-9862: Removed empty line from DataFlowXmlReader
* RSSL-9871: Add comments to RabbitMqTransformation
* RSSL-9871: Fixed gitlab-ci
* RSSL-9871: Fixed gitlab-ci.yml
* RSSL-9871: Fixed RabbitMqTransformation
* RSSL-9871: Fixed RabbitMqTransformation properties
* RSSL-9871: Fixed RabbitMqTransformation tests
* RSSL-9871: Fixed RabbitMqTransformation tests
* RSSL-9871: Fixed RabbitMqTransformation tests
* RSSL-9871: Fixed RabbitMqTransformation tests; Add more tests & comments
* RSSL-9871: Implemented RabbitMqTransformation and tests for it
* RSSL-9871: Implemented RabbitMqTransformation and tests for it
* RSSL-9871: Implemented RabbitMqTransformation properties
* Seperating documentation and demo project
* Set theme jekyll-theme-cayman
* Set theme jekyll-theme-slate
* Set theme jekyll-theme-slate
* Set up CI with Azure Pipelines
* Set up CI with Azure Pipelines
* Skip this test for now
* Small code cleanup
* Splitting up tests into smaller projects
* SQLite support now also for DBMerge
* Starting documemtation with docfx
* Support for Create Procedure
* Support for identity columns of type bigint - issue #40
* Switched to .Net Standard build.
* Switched to local mssql image, with fix for https://github.com/microsoft/mssql-docker/issues/355
* Switched to Microsoft SQLite implementation
* Switching attribute mapping for Lookup and Aggregation
* Switiching parameters for ReadTableDefinition
* SYSOPS-1035 changed nuget destination
* SYSOPS-1035 deploy #2
* SYSOPS-1035 test fix
* SYSOPS-1035 test push
* SYSOPS-1035 Стилистика
* TableDefinition can now be retrieved from table name for DBDest
* Test for Bulk Insert in SQLTask added.
* Test getDatabaseListTask repaird - assert was wrong
* test_build
* Testing Bulk insert operations
* Testing in docker and Gitlab
* Tests refactored except Big Data tests
* Todo updated
* Trying out docfx
* Uncommenting test code
* Unicode issue in docu
* Unit test debug
* Unit test debug
* Unit test debug
* Unit test debug
* Update _config.yml
* Update .gitignore
* Update .gitignore
* Update .gitlab-ci.yml
* Update .gitlab-ci.yml
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update azure-pipelines.yml
* Update azure-pipelines.yml
* Update azure-pipelines.yml for Azure Pipelines
* Update ChangeAction.cs
* Update CNAME
* Update dataflow_transformations.md
* Update dataflow_transformations.md
* Update docu
* Update docu
* Update docu
* Update docu and realease notes for 1.1.1
* Update docu, releasing 1.7.0
* Update getting_started.md
* Update index.md
* Update index.md
* Update issue templates
* Update issue templates
* Update LICENSE
* Update manifest
* Update overview_dataflow.md
* Update overview_dataflow.md
* Update overview_logging.md
* Update overview_logging.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update ReleaseNotes.md
* Update style.scss
* Update toc.yml
* Updated dependency to current versions (changes for CSVReader necessary)
* Updated documentation
* Updated layout
* Updated to .NET Core 2.2. Updated documentation.
* Updating ConnectionManager
* Updating doco
* Updating docs
* Updating docs with new comments
* updating docu
* updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu - now containing deletes for unnecessary files
* Updating docu & prep 1.7.3
* Updating docu and preparing 1.6.1
* Updating docu, small improvement for adding nlog config
* updating documentation for dataflow sources and destinations
* Updating documentation to 1.1.0-beta
* Updating documentation.
* Updating Nuget packages
* Updating package references
* Updating packages
* Updating packages to latest
* Updating project info
* Updating Readme
* Updating realease note and documentation
* Updating release note for packge
* Updating Release Notes
* updating test environment
* updating todo
* updating todo
* Updating todo
* Updating todo
* Updating todo list
* Updating TODO list
* Updating website
* Updating xml docs and cleaned up usings
* updating xml documentation
* Upgrade dependencies to modern SQLServer and PGSQL drivers
* Using Completion prop with await method instead Task.ContinueWith()
* Using CSVHelper object mapping functionality - see also issue #5
* Using ExpandoObject as default implementation
* Using the nuget package creation included in VS 2017
* XML docs adding. Also first attempts to use docfx to create api documentation and a project homepage.
* xml summary tags enhanced
* XmlSource with new base class
* Все юнит-тесты проходят
* Adding SkipRows Property to skip the first X rows in a source file before start reading.
* Cached connections in REST and Kafka transformations
* Fixed exception propagation in RowTransformation
* fixed gitlab-ci
* fixed paths to README and logo
* Version management moved from manual to dotnet tool Versionize
* **release:** 1.11.2-versionize.2
* **release:** 1.11.2-versionize.2

<a name="1.11.2-versionize.2"></a>
## 1.11.2-versionize.2 (2024-08-22)

### Other

* + установил версию 1.11.1
* Add code coverage
* Add Nuget publishing to Gitlab repo
* Add task name to custom destination + task name logging on LinkTo
* Add TConvert generic method type to IDataFlowLinkSource.LinkTo
* Added caching for nuget restored packages
* Added ConnectionManagerType to IConnectionManager interface.
* Added Culture to ITask interface, to be able to read culture for non-db tasks, like CSV
* Added missing QB, QE in SqlOdbcConnectionManager for BulkInsertSql.
* Added parameter support in complete SqlTask (NonQuery, Scalar)
* Added SupportDatabases, SupportProcedures, SupportSchemas, SupportComputedColumns to IConnectionManager. Configured MySql and SQLite. Adjusted different tasks to check for functionality support.
* Added test to improve coverage to get at least the same level as in 1.0.9
* Adding a VoidDestination. Updated docu.
* Adding AfterBatchInsert action
* Adding Aggregation component
* Adding artice about control flow tasks, also moving dataflow tasks to its own namespace.
* Adding async methods and improved sql parsing
* Adding async support (Issue #30)
* Adding attribute handling
* Adding base class for DataFlowDestination
* Adding BaseClass for DropTask
* Adding basic performance tests for LeaveOpen Connections (issue #39)
* Adding basic support for transactions
* Adding batchsize to DbMerge
* Adding bulk insert for Postgres
* Adding ColumnMap and ExcelColumn attributes
* Adding comment for access odbc tests
* Adding CompareColumn attribute for DBMerge (#42)
* Adding composite Keys to table creation and table definition
* Adding Connection Manager and basics
* Adding CrossJoin
* Adding csv source with no header
* Adding CSVDestination
* Adding DataFlowBatchDestination base class
* Adding DataFlowSource Base class
* Adding DataFlowTransformation base class
* Adding DBMerge & Updating docs
* Adding default date serialization - see Issue #34
* Adding DeleteColumn and support for delte in DBMerge
* Adding Delta information as Source for DBMerge
* Adding DeltaTable List to DBMerge
* Adding demo to new ETLBoxDocu project
* Adding docker setup infos
* Adding docu & fixes for JsonSource
* Adding docu for Control Flow and reorganizing articles
* adding documentation
* Adding dynamic for XmlSource
* adding dynamic object support for Sort
* Adding dynamic object to CSVDestination
* Adding dynamic type support for Json and Memory components
* Adding dynamic type to Lookup
* Adding environment var handling
* Adding error buffer to Lookup
* Adding error handling for csv source
* Adding error handling for custom destination
* Adding Error Linking
* Adding error linking for CSVDestination
* Adding error linking for custom source
* Adding ErrorHandling for Json
* Adding example code for multicast and webservice as source
* Adding example for Json and CustomDestination to documentation
* Adding example for reusable data flow
* Adding excel source error handling
* Adding ExcelSource for DataFlows
* Adding exception handling for faulted tasks
* Adding exception if no connection manager exists
* Adding exception tests if table does not exist
* Adding ExceptionHandling to DBSource (rebasing first)
* Adding excpetion handling for DBDestination
* Adding first draft for powershell cmdlet
* Adding full configuration support to have class maps registered in CsvSource (see also Issue #5)
* Adding GCPressure fix #59
* Adding generic CSVSource
* Adding generic task property copy method & fixing cast in dflinker for MergeJoin
* Adding http sources for JsonSource
* Adding HTTPClient for Stream Destinations
* Adding IDataFlowBatchDestination
* Adding IgnoreBlankRows support for ExcelSource - #52
* Adding improved support for special characters in table/schema names
* Adding improved TD support for MySql and sql server
* Adding JsonConverter support for multiple JsonProperty items
* Adding JsonConverter to support JsonPath
* Adding JsonDestination
* Adding JsonSource
* Adding LeaveOpen connection support - issue #39
* Adding legal information
* Adding logging for all DBs
* Adding logging for dataflow tasks
* Adding logging to duplicate example test
* adding Logo 32x32
* Adding memory source/destination tests
* Adding ModifyDBSettings prop to ConnManagers
* Adding more error tolerance to ExpandoJsonConverter
* Adding multicast destination to test (now works with 3 targets)
* Adding namespaces: ETLBox.ControlFlow, ETLBox.Logging, ETLBox.Helper, ETLBox.ConnectionManager
* Adding new feature: All destinations now will ignore nulls
* Adding newly introduced Support-Flags to AccessOdbcConnectionManager
* Adding nlog.config info
* Adding non generic implementation of DBDestination and RowTransformation (based on the sting array generic)
* Adding non generic implementations.
* Adding null handling for custom destination
* Adding nuspec for packaging
* Adding paginated uri
* Adding paramter usage for Odbc connector
* Adding performance test #59
* Adding PKConstraintName support for CreateTableTask #62
* Adding prototype MemoryDestination/Source
* Adding query parameter support for sql task
* Adding release notes
* Adding RowMultiplication transformation
* Adding slide deck
* Adding SQLite support for Dataflow
* Adding SQLite support for reading TableDefinition
* Adding SqlLite ConnectionManager
* Adding StartPostAll
* Adding string arrays as possible type for DBSource
* Adding support for comments in MySql
* Adding support for delta load and ExpandoObjects
* Adding support for dynamic object in ExcelSource
* Adding support for dynamic objects (ExpandoType only)
* Adding support for exists tasks
* Adding support for nested arrays in json files
* Adding support for partly string array
* Adding support for Postgres
* Adding support for reading table definition from access
* Adding support for Views as source in Sql Server
* Adding TableName property to DBSource
* Adding telling exceptions to CreateTableTask
* Adding test
* Adding test for #54
* Adding test for AfterBatchWrite
* Adding test for aggregation with dynamic object
* Adding test for asny completion
* Adding test for connection opening and pooling.
* Adding test for enum types
* Adding test for hash match
* Adding test for logging when executing async
* Adding test for merge with empty source
* adding test for non generic dbsource
* Adding test for writing into json with CustomDestination
* Adding test if DBSource uses a view
* Adding test that copies table based on existing TD for PR #60
* Adding test to check if symbol are excluded for releases
* Adding test to load data from access DB DataSource into Sql Server Data Source
* Adding test to verify GC Pressure #59
* Adding tests for aggregation with attributes
* Adding Tests for Connection Handling
* Adding tests for dynamic object on BlockTransformation
* Adding tests for dynamic support in custom source and destination
* Adding tests for Excel String Array, fixing FlatFile tests
* Adding tests for Issue/PR #4
* Adding tests for performance issue with DBMerge
* Adding tests for ReadLogTask
* Adding tests for special characters in ODBC
* Adding tests for Sql issues
* Adding tests to proof parallel execution of sql task.
* Adding VoidDestination predicate logic when linking - issue #33
* Adding xml destination support for dynamic
* Adding xml documentation
* Adding XmlSource draft
* Addomg log images
* Adjusting Configuration of CSVSource to CSVHelper Configuration
* Adjusting to renaming
* Aggregate with attributes - only one aggregation attribute
* Aggregation now accepts multiple attributes
* All tests debugged on Mac / Docker
* All tests green - initial version of Postgres support
* All tests pass
* Allow more than one beta to be published to nuget.org
* Allow tests to continue run on subsequent failures
* Allowing Arrays for JsonPath Expando Converter
* Almost all tests green for MySql support
* Apply 1 suggestion(s) to 1 file(s)
* Apply 1 suggestion(s) to 1 file(s)
* Apply 1 suggestion(s) to 1 file(s)
* Artifacts publishing fixed
* Avoiding race conditions
* Basic bulk insert support
* BlockTransformation now allows different type for output (See also issue #13)
* Build fixes
* Build fixes
* Build rules to always build on release branch
* changed EUR to $ sign
* Changed title in docu
* Changing default template
* Changing docs to new namespaces
* Changing docu - CurrentDbConnection to DefaultDbConnection
* Changing documentatin style
* Changing documentation
* Changing Post to sendasync.wait
* Changing SetValue to TrySetValue to avoid exceptions
* Changning ChangeAction from string to enum
* CI/CD debug
* CI/CD debug
* CI/CD debug
* cleaning up
* Cleaning up
* Cleaning up
* Code clenaup
* Code clenaup in demo project
* Code formatting & adding Create & Drop method
* Connection String class now also AdoMD compliant.
* ControlFlow tests pass
* CopyTaskProperties method improved
* Correcting interface defintions
* Create _config.yaml
* Create CNAME
* Create CNAME
* Create CNAME
* Create example_basics.md
* Create FUNDING.yml
* Create index.md
* Create pull_request_template.md
* Create style.scss
* Create todo.md
* CreateProcedureTasks now with MySql and Postgres
* Creating ErrorHander class
* Dataflow tests debug
* DBMerge now allows composite keys - issue #42
* DbMerge now supports Expandoobject
* DBSource now supports custom sql
* Debug flat files under gitlab
* Debug local gitlab run
* Debug tests on Kubernetes, somehow Regex works differently (case sensitive)
* Debug unit tests on Apple Silicon M1, most of tests pass
* Debugging CI build
* Default batch size now 1 for Csv and json
* Delete _config.yml
* Delete azure-pipelines.yml
* Delete index.md
* Delete style.scss
* DestinationTableDefinition safety checked and fallback to TableName
* dev/MLRSSL-1127 + стабилизация запуска пакета
* dev/MLRSSL-1127 + стабилизация запуска пакета
* dev/MLRSSL-1127 + стабилизация запуска пакета
* dev/MLRSSL-1127 + удалил лишний логгер из RestTransformation, удалил метод GetLogger
* DFStreamDestination base class added
* Doc generation is moved to doxygen
* Docs updated - now with Google Analytics.
* Documentation changed
* Documentation improved
* Documentation moved into docu project
* Documentation update
* Documentation update
* Documentation updated - new link to video added.
* Eliminated Sonarcube for now
* EOL LF -> CRLF for all .cs and .csv files
* Error Linking now accepts multiple sources
* Example code for Issue #5 - Duplicate check with RowTransformation and BlockTransformation
* Example dataflow added
* Example test added. Updating package version.
* Example test for Issue #6 (consuming a webservice with a custom source)
* Excel source now ignore blank rows
* ExcelSource now takes header row into account
* Expose destination completion tasks so several can be awaited at once
* Extending DBSource - columns are now mapped to property names. DBDestination now avoids Identity columns when matching prop Names
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: debug running tests
* feature/MLRSSL-1127: fix running separated test projects
* feature/MLRSSL-1127: fixed serialization tests project
* feature/MLRSSL-1127: fixt a pack of warnings, cklickhouse moved to target netstandard2.1, fixed a test of kafka
* feature/MLRSSL-1127: konfigure kafka on gitlab-cy
* feature/MLRSSL-1127: redesign test s of kafka from testcontainers to external service
* feature/MLRSSL-1127: remove debug running tests
* feature/MLRSSL-1127: remove logging a progress on dbdestination
* feature/MLRSSL-1127: remove unused file
* feature/MLRSSL-1127: return a single dotnet test and fix a progress
* feature/MLRSSL-1127: separated dotnet test for all projects
* feature/MLRSSL-1127: вернул запрет на артифакты если упали тесты
* feature/MLRSSL-1127: добавляю тесты в CI gitlab на мерж-реквесте
* feature/MLRSSL-1161: fix kafka test
* feature/MLRSSL-1161: fix warnings
* Finalizing Aggregation
* Finalizing CrossJoin
* Finalizing lookup
* Finalizing tests, adding last CF task for next version
* Finalizing transaction support
* First dataflow with Bulk Insert
* First draft for fluent LinkTo implementation - Issue #33
* First draft implementation on RowTransformation for automatic exception handling - #41
* First implementation of ODBC support and bulk Insert with ODBC
* First implementation of XmlDestination
* First prototype
* First prototype
* First protoype for DbMerge with Dynamic
* Fix "File not found" in unit tests
* Fix build
* Fix for Bulk Insert, improving SQLite support
* Fix for duplicated CI builds on merge requests
* Fix for duplicated CI builds on merge requests
* Fix for https://sonarqube.rapidsoft.ru/project/issues?id=open-source_etlbox_AX2rV2MSOqMd9uKUCs_O&open=AX2y9dSNqZnYS8uXZe1l&resolved=false&types=BUG
* Fix for Issue #5 - for the DBDestination, the name of the properties were not matched with the column names of the destination table.
* Fix one more "File not found"
* Fix publish rules
* Fix version auto-numbering
* Fix version auto-numbering
* Fix version auto-numbering
* fix:RSSL-10003 - remove vulnurabilities from dependencies
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10005 DbRowTransformation leaking connections
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* fix:RSSL-10023 - fix of RestTransformation and a deserialization on xml-encoded values
* Fixed bug :setting the value of the Enum property.
* Fixed bug :setting the value of the Enum property.
* Fixed bug in Uri output & fixing tests
* Fixed bug with ConnectionManager in DBSource.  Using a DBSource as non-generic class now works flawless.
* Fixed issue with data load into database in case of flat file.
* Fixed naming issue of constructor for Issue #17
* Fixed package Id to avoid conflict with EtlBox 2.x, 3.x
* Fixed version counter
* Fixes for single-processor execution under docker
* Fixes for SQL Server data types:
* Fixes to nuget.org publish procedure
* Fixing < issues
* Fixing ArgumentNulLException #54
* Fixing batch size bug when creating TableDef in DbDestination
* Fixing bug for only one ExcelColumn
* Fixing bug for only one ExcelColumn
* Fixing bug in DBMerge with higher amount of data
* Fixing bug in MemoryDestination when data exceeds batch size
* Fixing bug in multicast with readonly properties
* Fixing bug when reading TableDefinition in Postgres #48
* Fixing bug when using default db connection in ControlFlow object
* Fixing bug when using more excel columns than props
* Fixing bug with error when table name not in default schema
* Fixing connection issues with Access Db
* Fixing ControlFlow Tests
* Fixing database connector tests
* Fixing Issue #17 - sql without tabledefinition will work now
* Fixing issue when reading PK constraints on table with Index
* Fixing issue with different property sequences and ExpandoObject in DbDestination
* Fixing issues with ExpandoJsonConverter and CreateTableTask
* Fixing perf test for MemDest
* Fixing performance issue in DbMerge #54
* Fixing regex for ObjectNameDescriptor
* Fixing TableDefinition sql #43
* Fixing test error for null values in dynamic objects
* Fixing test for Bulk Insert with AccessOdbcConnectionManager
* Fixing tests - repairing changes to DbDestination
* fixing tests for DbMerge
* Fixing timestamp issue for Postgres #46
* Fixing typos
* Fixing xml comment issues
* GCPressure fix can be disabled now
* Gitlab test setup
* GitLab tests and collector
* Google analytics code added.
* Having task type only in base class
* Ignore null values of cross join output
* Implementation of DBMerge
* Implemented own completion logic
* Improved CF tasks for Access
* Improved connection handling for DbSource
* Improved error message when no column names could be parsed from DbSource #57
* Improved logging for Dataflow tasks.
* Improving constructor code DbMerge
* Improving ControlFlow SQLite support
* Improving DBMerge - adding IdColumn attribute (#42)
* Improving docu
* Improving docu
* Improving docu
* Improving docu
* Improving docu
* Improving docu for complex example
* Improving Exception handling
* Improving excpetion handling for Lookup and renaming CSVSource to CsvSource
* Improving lookup - testing shortcut for output equals input type
* Improving Performance Tests
* Improving readme
* Improving readme
* Improving SQLite support, improving BulkInserts
* Improving StartLoadProcessTask
* Improving tests
* Initial commit
* Initial commit
* Introducing RowDuplication
* Issue #18 Adding OnCompletion for CustomDestination
* Issue #5 - completing example test.
* Issue#22 Handle empty cells in ExcelSource
* json source with new base class
* JsonWriter отлажен с отправкой в HTTP POST
* Line endings modified for JSON
* Link to imprint repaired
* Link to imprint repaired again
* Logging refactored
* Making img path relative
* MemorySource now allows IEnumerable
* Merge branch 'bencassie-feature/linkto-type-convert' into dev
* Merge branch 'bencassie-feature/task-names' into dev
* Merge branch 'bugfix/RSSL-10003' into 'develop'
* Merge branch 'bugfix/RSSL-10005' into 'develop'
* Merge branch 'bugfix/RSSL-10023' into 'develop'
* Merge branch 'bugfix/RSSL-10023' into 'develop'
* Merge branch 'bugfix/RSSL-10023' of https://git.rapidsoft.ru/open-source/etlbox into bugfix/RSSL-10023
* Merge branch 'bugfix/RSSL-9416-3' into 'develop'
* Merge branch 'bugfix/RSSL-9416-egors-pk' into 'bugfix/RSSL-9416-egors'
* Merge branch 'bugfix/RSSL-9416-egors' into 'develop'
* Merge branch 'bugfix/RSSL-9416' into 'develop'
* Merge branch 'bugfix/RSSL-9416' into 'develop'
* Merge branch 'bugfix/RSSL-9416' into 'develop'
* Merge branch 'bugfix/RSSL-9851' into 'develop'
* Merge branch 'bugfix/RSSL-9862' into 'develop'
* Merge branch 'bugfix/set_version' into 'develop'
* Merge branch 'Completion_Complex_Graphs' into dev
* Merge branch 'dev'
* Merge branch 'dev'
* Merge branch 'dev'
* Merge branch 'dev' into dev
* Merge branch 'dev' of https://github.com/roadrunnerlenny/etlbox into dev
* Merge branch 'dev/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1137-refactor' into 'feature/MLRSSL-1137'
* Merge branch 'dev/MLRSSL-1153' into 'develop'
* Merge branch 'dev/MLRSSL-1161-1' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-2' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-3' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-merge' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk-logs' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk-logs' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161-egors'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161-egors'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161-pk' into 'feature/MLRSSL-1161'
* Merge branch 'dev/MLRSSL-1161' into 'feature/MLRSSL-1161'
* Merge branch 'dev/RSSL-8664' into 'master'
* Merge branch 'dev/RSSL-8664' into 'master'
* Merge branch 'dev/RSSL-9421-3' into 'develop'
* Merge branch 'dev/RSSL-9421-serializer' into 'develop'
* Merge branch 'dev/RSSL-9421-tests' into 'develop'
* Merge branch 'dev/RSSL-9851' into 'feature/MLRSSL-1255'
* Merge branch 'develop' into dev/RSSL-9421-tests
* Merge branch 'develop' into feature/MLRSSL-1255
* Merge branch 'feature/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1127' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1127' into 'pass_tests'
* Merge branch 'feature/MLRSSL-1137' into 'dev/MLRSSL-1137-refactor'
* Merge branch 'feature/MLRSSL-1137' into 'develop'
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1154' into feature/MLRSSL-1161
* Merge branch 'feature/MLRSSL-1161-egors' into 'feature/MLRSSL-1161'
* Merge branch 'feature/MLRSSL-1161' into 'develop'
* Merge branch 'feature/MLRSSL-1161' into 'feature/MLRSSL-1127'
* Merge branch 'feature/MLRSSL-1161' into 'pass_tests'
* Merge branch 'feature/MLRSSL-1161' into dev/MLRSSL-1161-2
* Merge branch 'feature/MLRSSL-1161' into feature/MLRSSL-1161-egors
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1161' into pass_tests
* Merge branch 'feature/MLRSSL-1255' into 'develop'
* Merge branch 'feature/RSSL-9862' into 'develop'
* Merge branch 'ForkImprovements' into dev
* Merge branch 'hotfix/MLRSSL-1002' into 'master'
* Merge branch 'hotfix/package-id' into 'master'
* Merge branch 'hotfix/package-id' into 'master'
* Merge branch 'hotfix/RSSL-8664' into 'master'
* Merge branch 'hotfix/SYSOPS-1035' into 'master'
* Merge branch 'Improving_Lookup' into dev
* Merge branch 'Issue1' into release
* Merge branch 'master' into dev
* Merge branch 'master' into develop
* Merge branch 'master' into develop
* Merge branch 'master' into release/1.10
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'master' of https://github.com/roadrunnerlenny/etlbox
* Merge branch 'pass_tests' into feature/MLRSSL-1161
* Merge branch 'RefactoringTests' into dev
* Merge branch 'release/1.10' into 'develop'
* Merge branch 'release/1.10' into 'master'
* Merge branch 'release/1.8.10' into 'master'
* Merge branch 'release/1.9.1' into 'master'
* Merge branch 'versionize' into 'develop'
* Merge of changes on getting started
* Merge pull request #1 from mukundnc/mukundnc-patch-1
* Merge pull request #20 from bruce-dunwiddie/dev
* Merge pull request #21 from roadrunnerlenny/dev
* Merge pull request #38 from bencassie/fix/dbdestination-taskname-null-check
* Merge pull request #4 from mukundnc/master
* Merge pull request #60 from shokurov/dev
* Merge pull request #63 from vladislav-smr/dev
* Merge pull request #65 from vladislav-smr/dev
* Merge pull request #66 from vladislav-smr/dev
* Merge pull request #67 from SipanOhanyan/patch-1
* Merge pull request #68 from SipanOhanyan/patch-3
* Merge pull request #69 from SipanOhanyan/patch-4
* Merge remote-tracking branch 'origin/dev/MLRSSL-1161-merge' into dev/MLRSSL-1161-merge
* Merge remote-tracking branch 'origin/develop' into dev/MLRSSL-1153
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1154
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1154
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1154
* Merge remote-tracking branch 'origin/develop' into feature/MLRSSL-1161
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161-egors' into feature/MLRSSL-1161-egors
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into dev/MLRSSL-1161-3
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into dev/MLRSSL-1161-pk-logs
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into feature/MLRSSL-1161-egors
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1161' into pass_tests
* Merge remote-tracking branch 'origin/feature/MLRSSL-1255' into dev/RSSL-9851
* Merge remote-tracking branch 'origin/gitlab-build' into gitlab-build
* Merge remote-tracking branch 'origin/master' into hotfix/RSSL-8664
* Migrate tests to net5.0
* Migrate to net6.0
* Minor code style changes
* Minor docu updates
* Minor fixes to xml comments
* MLRSSL-1002: Fixed create table task on PostgreSql
* MLRSSL-1002: Fixed create table task on PostgreSql - bigint identity changed from serial to generated by default as identity
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1127 - ContactDatabase. Разработка ETL пакета по онлайн-импорту событий из топика Kafka
* MLRSSL-1128 Доработать ETLBox для репликации данных из Clientrix и БДК
* MLRSSL-1134 PoC. Разработать шаг ETL для DataFlow через EtlBox
* MLRSSL-1134 PoC. Разработать шаг ETL для DataFlow через EtlBox
* MLRSSL-1135 PoC for Kafka DataflowSource
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fix of JsonTransformation
* MLRSSL-1137 + fixed start container for kafka
* MLRSSL-1137 + добавил логирование Url и Body rest-запроса
* MLRSSL-1137 + добавил логирование Url и Body rest-запроса
* MLRSSL-1137 + добавил реализацию агрегации через настройку мэппингов
* MLRSSL-1137 + отменил изменения JsonTransformation
* MLRSSL-1137 + починил JsonTransformation
* MLRSSL-1137 + починил JsonTransformation, добавил режим обработки не найденных полей в ScriptedRowTransformation
* MLRSSL-1137 + правка по комментам к мержу
* MLRSSL-1137 + правки по комментам к мержу
* MLRSSL-1137 + удалил вызов logger.BeginScope
* MLRSSL-1137 Refactored Mappings implementation for Aggregation
* MLRSSL-1153 A transformation step has been developed based on Microsoft.CSharpScript
* MLRSSL-1153 Added nested dynamics to the test
* MLRSSL-1153 Added type check guard for anonymous types
* MLRSSL-1153 Improved error reporting, added error line and pointer to the compilation error message
* MLRSSL-1154 Fixed namespaces back
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + добавил DbTransformation
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + добавил DbTransformation
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + доработал создание пакетов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + доработал создание пакетов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + доработал создание пакетов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + закомментил SQLite
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поменял таргеты на netstandard2.0
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил namespaces
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил namespaces
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил namespaces
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поправил тест
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + поубирал часть варнингов
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + пофиксил десериализацию
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + правки по комментам к мержу
* MLRSSL-1154 Заменить логирование EtlBox на Microsoft.Extensions.Logging + удалил перенесенные тесты
* MLRSSL-1161 + add a IList deserialization
* MLRSSL-1161 + added test
* MLRSSL-1161 + added test
* MLRSSL-1161 + added test
* MLRSSL-1161 + fix a deserialization
* MLRSSL-1161 + fix a JsonTransformation
* MLRSSL-1161 + fix a JsonTransformation
* MLRSSL-1161 + fix a JsonTransformation, add test
* MLRSSL-1161 + fix loading libraries into memory
* MLRSSL-1161 + remove unused namespace
* MLRSSL-1161 + rename test
* MLRSSL-1161 + renamed test
* MLRSSL-1161 + return error logging logic
* MLRSSL-1161 + починил логирование в Etl, доработал RestTransformation
* MLRSSL-1161 + починил тесты
* MLRSSL-1161 + починил тесты
* MLRSSL-1161 Added TreatWarningsAsErrors for all builds
* MLRSSL-1161 Added TreatWarningsAsErrors for CI builds
* MLRSSL-1161 Added TreatWarningsAsErrors for CI builds
* MLRSSL-1161 EtlBox: добавить возможность безопасно прервать процесс DataFlow
* MLRSSL-1161 EtlBox: добавить возможность безопасно прервать процесс DataFlow
* MLRSSL-1161 Fixed all tests, except TestDatabaseConnectors
* MLRSSL-1161 Fixed Kafka tests running within single client group
* MLRSSL-1161 Fixing remaining tests
* MLRSSL-1161 Refactored warnings, moved REST tests to a separate project
* MLRSSL-1161 Refactoring CI CD
* MLRSSL-1161 Refactoring CI CD
* MLRSSL-1161 Refactoring CI CD
* MLRSSL-1255 + PoC SqlQueryTransformation
* More styles need to be overwritten
* Most parts refactored
* Mostly converted to net core. Still issues with nlog that need to be fixed.
* Mostly logging refactored
* Moved ETLBoxCmdlets into its own repo
* Moving _site to /docs
* Moving config helper class into test shared library
* Moving connection manager tests into separate Test Project
* Moving CsvSource to new base class
* Moving ETLBoxDemo into it's own repo
* nlog now properly setup, all tests green.
* Non generic implementation for lookup
* Now full support for SQLite
* Now returning column name as opposed to possibly escaped column name from raw text.
* Odbc and AccessOdbc Connection Manager added, including tests.
* Odbc support - adding Odbc connection string and some tests. Odbc bulk insert replacement is not available yet.
* One more fix to changed SQLite datetime formats
* Overhauling complete logging
* Overwrite of .navbar-inverse now with !important rule
* Overwriting navbar-inverse - minor change to docs
* Performance tests, adjusting ReleaseGCPressure for faster execution speed
* Pre-release 1.7.5
* Preparation release 1.4.1
* Preparing 1.6.5, improving MemoryDestination
* Preparing 1.7.1
* Preparing DataFlowLinker
* Preparing for Odbc Issue
* Preparing for release
* Preparing v. 1.6.2
* Preparing v1.7.4, changing back to .NET Standard 2.0
* Preparing v1.7.7
* Preparing v1.8.1
* Preparing v1.8.2, updating docu
* Preparing v1.8.7
* Preparing version 1.6.0
* Preparing version 1.6.3
* Preparing version 1.8.4
* Preparing version 1.8.6
* Preparing version 1.8.8
* Preparing version 1.8.8 alpha
* Pulling up test coverage
* Pulling up test coverage
* Recreating documents with new namespace help
* Refactoring base classes dataflow
* Refactoring constructor logic
* Refactoring ControlFlow and Logging Tests
* Refactoring DataFlow Tests
* Refactoring DBSource + adding new test for multiple sources into one destination
* Refactoring DF Destination
* Refactoring LinkTo into composite class
* Refactoring RowDuplication/RowMultiplication by using TransformationManyBlock
* Refactoring TypeInfo
* Refactorting and improving ConnectionStrings
* Refining ControlFlowTasks for Postgres
* Release 1.4.0
* Release notes - added DBSource
* Release notes improved
* Releasing new docu to website
* Releasing Version 1.5.0
* Releasing version 1.7.5
* Remove unnecessary logging
* Removed ConnectionManagerSpecifics class.
* Removed unnecessary test
* Removed unneeded props
* Removing a !NOTE md
* Removing CleanUpLogTask
* Removing clutter
* Removing deprecated sql extension code
* Removing Execute method from GenericTask
* Removing FileConnectionManager & smaller renamings
* Removing FileGroupTask
* Removing IDbConnectionManager
* Removing old test project
* Removing supefluous lines
* Removing symbols file for Release version
* removing unnecessary ref
* removing unnecessary ref keyword
* Removing unnecessary usage of Command in DbTask
* Removing useless IsConnectionOpen-Check
* Rename _config.yaml to _config.yml
* Renamed "Delete" to "Drop" in DropDatabaseTask
* Renaming ConnectionString to SqlConenctionString, adjusting tests to name changes
* Renaming CSV/DB to Csv/Db in Tests and docu (Search & Replace)
* Renaming CSVSource/CSVDest to CsvSource/CsvDest
* Renaming DBSource & DBDestination to DbSource and DbDestination
* Renaming ExecuteAsync To PostAll
* Renaming Issue3 testcases
* renaming ReadTopX to Limit
* Renaming TableNameDescriptor, improving queries with new ObjectNameDescriptor
* Renaming Test
* Renaming test directories
* Renaming test projects, continuing with refactor DF tests
* Renaming tests for former non generic
* Renaming to project standard. Step 2
* Renaming to project standard. Step 2
* Repairing API documentation
* Repairing demo
* Repairing tests - avoiding database locked error and skipping not maintained tests
* Repairing toc.yml
* Replacing mdb with accdb files
* Resetting batch size to 1000 for json, csv and memory dest
* Resolving Access ODBC connections issues in tests
* Resolving GC pressure
* Revert "MLRSSL-1137 + добавил логирование Url и Body rest-запроса"
* Rewriting basic example
* RSSL-8664 Added coverage analysis
* RSSL-8664 Added publishing to nuget.org (only -beta and release versions)
* RSSL-8664 Additional clean up
* RSSL-8664 Bumped all dependencies to current versions
* RSSL-8664 bumped version in master
* RSSL-8664 CI test debug
* RSSL-8664 Code Cleanup
* RSSL-8664 Coverage report regex fixed
* RSSL-8664 CsvHelper updated to v.30
* RSSL-8664 Debugging CI test_job
* RSSL-8664 Debugging CI test_job
* RSSL-8664 Debugging one more unit test, where source and destination used the same connection
* RSSL-8664 Debugging parallel and repetitive test runs
* RSSL-8664 Fix for nuget publishing
* RSSL-8664 Fix nuget.org publishing
* RSSL-8664 Fixed nuget.org publishing to manual
* RSSL-8664 Fixing tests, refactoring, eliminate compiler warnings
* RSSL-8664 MySQL corner case unit test fixed
* RSSL-8664 Packaging and docs
* RSSL-8664 Readme.md updated with new badges
* RSSL-8664 Removed comments on local ms sql container
* RSSL-8664 Rename CSV -> Csv, step 1
* RSSL-8664 Rename CSV -> Csv, step 2
* RSSL-8664 Rename CSV -> Csv, step 3
* RSSL-8664 Unit test debug
* RSSL-8664 Updated README.md
* RSSL-8664 Настройка публикации NUget
* RSSL-8664 Отладка публикации на nuget.org
* RSSL-9416 Add a test for json serialization of property
* RSSL-9416 Added reference extensibility to scripting transformation
* RSSL-9416 CLONE - PoC: Добавить респондента в опросы, и сформировать для каждого уникальную ссылку
* RSSL-9416 Fix formatting
* RSSL-9416 Fix test name
* RSSL-9416: Adapt ScriptedTransformation to receive an assembly relative path
* RSSL-9416: Adapt ScriptedTransformation to receive an assembly relative path
* RSSL-9416: Add a default constructor to DbMerge, add test
* RSSL-9416: Add an ability to Process Newid and JsonSerialization to ScriptedRowTransformation
* RSSL-9416: Add check parameters for null to RestMethodInternalAsync
* RSSL-9416: Fix and error on desereialize non-generic interfaces
* RSSL-9416: Fix serialization
* RSSL-9416: Fix serialization, add test
* RSSL-9416: Fixed DbMerge
* RSSL-9416: Fixed DbMerge & DbTransformation deserialization
* RSSL-9416: Fixed IDataFlowSource (add ILinkErrorSource interface inheritance)
* RSSL-9416: Simplified tests
* RSSL-9416: Simplified tests
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 - CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421 CLONE - Разработать шаг ETL для вызова REST-метода по CSV-файлу
* RSSL-9421: Fixed HttpStatusCodeException
* RSSL-9421: Fixed RestTransformation & DataFlowXmlReader, improved tests
* RSSL-9421: Implemented DataFlow serialization extensions (read from xml) & tests
* RSSL-9421: Implemented RestTransformationTests
* RSSL-9421: Implemented RestTransformationTests
* RSSL-9421: Implemented RestTransformationTests
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9501 CLONE - EtlBox: добавить возможность безопасно прервать процесс DataFlow
* RSSL-9851 - add a comments and rename a test
* RSSL-9851 - add a table definition for complex custom select
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9851 -CLONE - ETL. Отправка транзакций в очередь
* RSSL-9862: Add new tests for DataFlowXmlReader
* RSSL-9862: Fixed DataFlowXmlReader (GetValue)
* RSSL-9862: Fixed DataFlowXmlReader (GetValue) & TypeExtensions; Add new tests
* RSSL-9862: Fixed DataFlowXmlReader extensions
* RSSL-9862: Fixed DataFlowXmlReader tests
* RSSL-9862: Fixed DataFlowXmlReader tests
* RSSL-9862: Fixed tests for DataFlowXmlReader
* RSSL-9862: Fixed tests for DataFlowXmlReader
* RSSL-9862: Removed empty line from DataFlowXmlReader
* RSSL-9871: Add comments to RabbitMqTransformation
* RSSL-9871: Fixed gitlab-ci
* RSSL-9871: Fixed gitlab-ci.yml
* RSSL-9871: Fixed RabbitMqTransformation
* RSSL-9871: Fixed RabbitMqTransformation properties
* RSSL-9871: Fixed RabbitMqTransformation tests
* RSSL-9871: Fixed RabbitMqTransformation tests
* RSSL-9871: Fixed RabbitMqTransformation tests
* RSSL-9871: Fixed RabbitMqTransformation tests; Add more tests & comments
* RSSL-9871: Implemented RabbitMqTransformation and tests for it
* RSSL-9871: Implemented RabbitMqTransformation and tests for it
* RSSL-9871: Implemented RabbitMqTransformation properties
* Seperating documentation and demo project
* Set theme jekyll-theme-cayman
* Set theme jekyll-theme-slate
* Set theme jekyll-theme-slate
* Set up CI with Azure Pipelines
* Set up CI with Azure Pipelines
* Skip this test for now
* Small code cleanup
* Splitting up tests into smaller projects
* SQLite support now also for DBMerge
* Starting documemtation with docfx
* Support for Create Procedure
* Support for identity columns of type bigint - issue #40
* Switched to .Net Standard build.
* Switched to local mssql image, with fix for https://github.com/microsoft/mssql-docker/issues/355
* Switched to Microsoft SQLite implementation
* Switching attribute mapping for Lookup and Aggregation
* Switiching parameters for ReadTableDefinition
* SYSOPS-1035 changed nuget destination
* SYSOPS-1035 deploy #2
* SYSOPS-1035 test fix
* SYSOPS-1035 test push
* SYSOPS-1035 Стилистика
* TableDefinition can now be retrieved from table name for DBDest
* Test for Bulk Insert in SQLTask added.
* Test getDatabaseListTask repaird - assert was wrong
* test_build
* Testing Bulk insert operations
* Testing in docker and Gitlab
* Tests refactored except Big Data tests
* Todo updated
* Trying out docfx
* Uncommenting test code
* Unicode issue in docu
* Unit test debug
* Unit test debug
* Unit test debug
* Unit test debug
* Update _config.yml
* Update .gitignore
* Update .gitignore
* Update .gitlab-ci.yml
* Update .gitlab-ci.yml
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update .gitlab-ci.yml file
* Update azure-pipelines.yml
* Update azure-pipelines.yml
* Update azure-pipelines.yml for Azure Pipelines
* Update ChangeAction.cs
* Update CNAME
* Update dataflow_transformations.md
* Update dataflow_transformations.md
* Update docu
* Update docu
* Update docu
* Update docu and realease notes for 1.1.1
* Update docu, releasing 1.7.0
* Update getting_started.md
* Update index.md
* Update index.md
* Update issue templates
* Update issue templates
* Update LICENSE
* Update manifest
* Update overview_dataflow.md
* Update overview_dataflow.md
* Update overview_logging.md
* Update overview_logging.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update ReleaseNotes.md
* Update style.scss
* Update toc.yml
* Updated dependency to current versions (changes for CSVReader necessary)
* Updated documentation
* Updated layout
* Updated to .NET Core 2.2. Updated documentation.
* Updating ConnectionManager
* Updating doco
* Updating docs
* Updating docs with new comments
* updating docu
* updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu
* Updating docu - now containing deletes for unnecessary files
* Updating docu & prep 1.7.3
* Updating docu and preparing 1.6.1
* Updating docu, small improvement for adding nlog config
* updating documentation for dataflow sources and destinations
* Updating documentation to 1.1.0-beta
* Updating documentation.
* Updating Nuget packages
* Updating package references
* Updating packages
* Updating packages to latest
* Updating project info
* Updating Readme
* Updating realease note and documentation
* Updating release note for packge
* Updating Release Notes
* updating test environment
* updating todo
* updating todo
* Updating todo
* Updating todo
* Updating todo list
* Updating TODO list
* Updating website
* Updating xml docs and cleaned up usings
* updating xml documentation
* Upgrade dependencies to modern SQLServer and PGSQL drivers
* Using Completion prop with await method instead Task.ContinueWith()
* Using CSVHelper object mapping functionality - see also issue #5
* Using ExpandoObject as default implementation
* Using the nuget package creation included in VS 2017
* XML docs adding. Also first attempts to use docfx to create api documentation and a project homepage.
* xml summary tags enhanced
* XmlSource with new base class
* Все юнит-тесты проходят
* Adding SkipRows Property to skip the first X rows in a source file before start reading.
* Cached connections in REST and Kafka transformations
* fixed gitlab-ci
* fixed paths to README and logo
* Version management moved from manual to dotnet tool Versionize
* **release:** 1.11.2-versionize.2

<a name="1.11.1"></a>
## 1.11.1 (2024-08-20)
✨ Features
* New transformation: SqlRowTransformation, SqlCommandTransformation to run parametrised SQL queries/commands
* New transformation: KafkaTransformation producing to Kafka topics
* New transformation: RabbitMqTransformation publishing to RabbitMq queues
* Improvement: RestTransformation now returns the response body as a string and HTTP code

🐛 Bug Fixes
* DataFlowXmlReader: Fix to allow `<[CDATA[..]]>` in XML data
* DataFlowXmlReader: Added support for floating point properties
* DbRowTransformation: Fixed connection leak
* Updated dependencies with vulnerabilities 

Other changes
* Migrated from manual versioning to [versionize](https://github.com/versionize/versionize)
* Update README.md

<a name="1.10.0"></a>
## 1.10.0 (2024-05-16)
✨ Features
* Added cancelation support for long running data flow processes
* New connection type: Added support for [Clickhouse](https://clickhouse.com/docs/) columnar store
* New source: Added [Kafka](https://kafka.apache.org/) topic support as a source
* New transformation: RestTransformation to post data to a REST endpoint and get results
* New transformation: JsonTransformation to evaluate Json path expressions and extract data from Json
* New transformation: ScriptedRowTransformation to evaluate C# expressions to transform data

Other changes
* DbTransformation renamed to DbRowTransformation (DbTransformation is kept as `Obsolete`)
* NLog replaced with Microsoft.Extensions.Logging (except when logs are written to DB table, NLog is kept as internal implementation)

<a name="1.9.1"></a>
## 1.9.1 (2023-06-18)
✨ Features
* Added DataFlowXmlReader, allowing saving data flow graph configuration as XML
