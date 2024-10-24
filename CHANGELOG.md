# Change Log

All notable changes to this project will be documented in this file. See [versionize](https://github.com/versionize/versionize) for commit guidelines.

<a name="1.11.10"></a>
## 1.11.10 (2024-09-12)

🐛 Bug Fixes
* Fixed a bug in `ScriptedRowTransformation` where the dependency injection was not working properly.

<a name="1.11.7"></a>
## 1.11.7 (2024-08-24)
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
