# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.0.0] 2018-08-28
### Added
- PNDA-4431: Add basic platform test for Flink in PNDA
- PNDA-4754: Add new metric for list of available kafka topics

### Changed
- PNDA-4408: Use kafka-python 1.3.5 in kafka test module
- PNDA-4481: Change Zookeeper status once loosing nodes when there is a zk quorum
- PNDA-4673: Use JDBC instead of PyHive to query Hive because PyHive does not support HTTP transport
- PNDA-4729: Remove unused per-topic health metric
- PNDA-4899: Introduce conditionality to produce build/mirror with only artefacts required

### Fixed
- PNDA-4106: Console not reflecting which zookeeper is lost
- PNDA-4223: Limit data read in test by specifying end timestamp
- PNDA-4702: Mock popen to prevent unit tests attempting to use nc which may not be installed
- PNDA-4714: Clean up hive connection resources

## [0.5.0] 2018-02-10
### Added
- PNDA-3391: Add more metrics to kafka plugin which is now managed in a by a JMX config file

### Changed
- PNDA-3601: Disable emailtext in Jenkins file and replace it with notifier stage and job
- PNDA-2282: Improved error reporting for Deployment Manager tests

### Fixed
- PNDA-3257: Code quality improvements

## [0.4.0] 2017-11-24
### Added
- PNDA-ISSUE-42: opentsdb platform test to return empty list in causes field for good health in opentsdb.health metric
- PNDA-2445: Support for Hortonworks HDP hadoop distro
- PNDA-2163: Support for OpenTSDB Platform testing
- PNDA-3381: Support for multiple Kafka endpoints

## [0.3.3] 2017-08-01
### Changed
- PNDA-3106: Publish per topic health

## [0.3.2] 2017-07-10
### Changed
- VPP-17: Change platform-tests from starbase to happybase which is more performant. Also don't create and delete a table as part of the hbase test as this causes the regionserver to leak java heap space.

## [0.3.1] 2017-06-28
### Changed
- PNDA-2672: Explicitly specify usage of CM API version 11
- PNDA-3088: Manage unclean leader election by adding a threshold
- PNDA-2940: Ensure data is not composed past the server limit

### Fixed
- PNDA-2597: Delete HBase folder from HDFS when blackbox test fails as part of cleanup to cope with HBase errors.

## [0.3.0] 2017-01-20
### Changed
- PNDA-2485: Pinned all python libraries to strict version numbers

## [0.2.0] 2016-12-12
### Changed
- Externalized build logic from Jenkins to shell script so it can be reused
- Merge kafka blackbox and whitebox & rename zookeeper_blackbox to zookeeper

## [0.1.1] 2016-09-13
### Changed
- Enhanced CI support

## [0.1.0] 2016-07-01
### First version

## [Pre-release]
### Added

- Plugins and core structure
	- Kafka plugin
	- Kafka blackbox plugin
	- Zookeeper plugin
	- CDH plugin
	- CDH blackbox plugin
	- DM plugin
- Unit tests
