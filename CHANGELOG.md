# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]
### Added
- PNDA-2445: Support for Hortonworks HDP hadoop distro

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
