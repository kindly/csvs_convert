
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.7.13] - 2023-01-22

### New

- `force_string` option for describers that will force the type to be string.

## [0.7.12] - 2023-01-21

### New

- `dump_file` option which will create dump files for `psql` and `sqlite3` cli tools.

## [0.7.11] - 2022-01-07

### Fixed

- WASM dependancy loop

## [0.7.10] - 2023-01-06

### Fixed

- Truncate for too large xlsx cell.

## [0.7.9] - 2023-01-01

### Changed

- Parquet now detects dates.

## [0.7.8] - 2022-12-24

### Changed

- Leading zeros in numbers and floats count as string

## [0.7.7] - 2022-12-23

### Changed

- Allow wasm for type detection

## [0.7.6] - 2022-12-19

### Fixed

- Fixed parquet bool errors

## [0.7.5] - 2022-12-18

### Changed

- Upgrade all dependancies and remove features not used in deps. 

## [0.7.2] - 2022-12-14

### New

- `threads` option to speed up stats and type guessing.

## [0.7.1] - 2022-12-07

### New

- `stats` and `stats_csv` options to make stats about the data.
- `csvs_to_*` commands not return the datapackage as a python dict insead of None.

## [0.7.0] - 2022-12-07

### New

- Changed name to `csvs_convert`
- All conversions now accept a list of `CSV` files.
- Type guessing for `CSV` files generating a `datapackage.json` file.

## [0.5.2] - 2022-07-27

### New

- environment var postgres support

## [0.5.0] - 2022-07-20

### New

- postgres support 

## [0.4.0] - 2022-05-14

### New

- XLSX support 
- Allow options too be passed to rust library
- Docs and tests in python libary