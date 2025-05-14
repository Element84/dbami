# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.4.0] - 2025-05-14

### Added

- `migration_lock` context manager that yields a database connection after obtaining
  an exclusive session-level advisory lock. ([#14])
- `--no-lock` option to the `migrate`, `rollback`, and `up` CLI commands. ([#14])
- `--lock-timeout` option to the `migrate`, `rollback`, and `up` CLI commands. ([#14])
- **Experimental** `MigrationHelper` utilty class to assist in obtaining an advisory
  lock, revoking connect privileges from other users, waiting for other connections to
  close, and terminating other connections prior to running migrations. ([#14])

### Changed

- Database migrations default to obtaining an advisory lock before running. ([#14])

## [v0.3.1] - 2025-02-04

### Added

- Type annotations on some DB functions ([#13])

### Changed

- pre-commit minimum version now 4.0 ([#13])

## [v0.3.0] - 2023-08-01

### Changed

- Connection reuse support extracted into `get_db_connection`, now all
  invocations of `get_db_connection` automatically support connection reuse.
  ([#11])

## [v0.2.0] - 2023-06-22

### Added

- `list-fixtures` command ([#10])
- `load-fixture` command ([#10])
- `DB.execute_sql()` method for running arbitrary SQL against a database ([#10])
- `execute-sql` command ([#10])

### Changed

- Schema version when applying `schema.sql` comes from max migration version,
  removing the need to track the version table and update it in that file. ([#10])

## [v0.1.0] - 2023-05-25

Initial release

[unreleased]: https://github.com/element84/dbami/compare/v0.4.0...main
[v0.4.0]: https://github.com/element84/dbami/compare/v0.3.1...v0.4.0
[v0.3.1]: https://github.com/element84/dbami/compare/v0.3.0...v0.3.1
[v0.3.0]: https://github.com/element84/dbami/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/element84/dbami/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/element84/dbami/tree/v0.1.0

[#10]: https://github.com/Element84/dbami/pull/10
[#11]: https://github.com/Element84/dbami/pull/11
[#13]: https://github.com/Element84/dbami/pull/13
[#14]: https://github.com/Element84/dbami/pull/14
