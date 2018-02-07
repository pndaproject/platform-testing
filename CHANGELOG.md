# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
- PNDA-3391: Add more metrics to kafka plugin which is now managed in a by a JMX config file

### Changed
- PNDA-3601: Disable emailtext in Jenkins file and replace it with notifier stage and job
- PNDA-2282: Improved error reporting for Deployment Manager tests

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
