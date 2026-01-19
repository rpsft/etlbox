# Change Log

All notable changes to this project will be documented in this file.

<a name="1.15.3"></a>
# 1.15.3
✨ Features
* Improvement: `AIBatchTransformation` now supports `PromptParameters` string setting, that contains json dictionary with custom parameters for liquid-based Propmt template.

<a name="1.15.2"></a>
# 1.15.2
✨ Features
* New library: `ETLBox.AI` to apply AI features to `DataFlow`
* New transformation: `AIBatchTransformation` to post prompt data to a [OpenAI](https://openai.com/) API endpoint and get results

<a name="1.13.3"></a>
# 1.13.3
✨ Features
* Improvement: Added `BoundedCapacity` to `DataFlowBatchDestination` options to restrict buffer size and max memory consumption

🐛 Bug Fixes
* Fixed a memory leak when connection managers were not owned and not disposed.
* Fixed a bug in `ScriptedRowTransformation` where the dependency injection was not working properly.

Other changes
* Moved back from [versionize](https://github.com/versionize/versionize) to scripted version bump in CI/CD pipeline

<a name="1.13.1"></a>
## 1.13.1 (2025-03-10)
Other changes
* Version bump and release preparation

<a name="1.13.0"></a>
## 1.13.0 (2025-03-10)
✨ Features
* Enhanced data flow process with connection manager pooling for better resource management
* Improved memory management and connection disposal

🐛 Bug Fixes
* Fixed vulnerabilities in dependencies (RSSL-10261)
* Added proper connection manager disposal to prevent memory leaks

Other changes
* Improved test debugging under .NET 8 SDK
* Updated documentation and TODO items

<a name="1.12.4"></a>
## 1.12.4 (2024-09-30)
Other changes
* Build improvements and dependency updates

<a name="1.12.3"></a>
## 1.12.3 (2024-09-28)
Other changes
* Added script to append GitLab changelog trailer to commits
* CI/CD pipeline improvements

<a name="1.12.2"></a>
## 1.12.2 (2024-09-28)
🐛 Bug Fixes
* Removed duplicating `<Version>` tags from project files

<a name="1.12.1"></a>
## 1.12.1 (2024-09-28)
Other changes
* Updated CI pipeline to handle version bump commits and renamed deploy job

<a name="1.12.0"></a>
## 1.12.0 (2024-09-28)
Other changes
* Added version bump script and updated CI pipeline configuration
* Improved CI/CD automation

<a name="1.11.11"></a>
## 1.11.11 (2024-09-26)
Other changes
* Updated CHANGELOG.md and documentation

<a name="1.11.10"></a>
## 1.11.10 (2024-09-12)
Other changes
* Minor release with internal improvements

<a name="1.11.9"></a>
## 1.11.9 (2024-08-24)
Other changes
* Minor release with internal improvements

<a name="1.11.8"></a>
## 1.11.8 (2024-08-24)
Other changes
* Minor release with internal improvements

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
