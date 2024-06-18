
# Change Log
All notable changes to this project will be documented in this file.
 
The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.8.10] - 2024-06-18

### New

- Truncate option

### Fixed

- More date formats for postgres being used

## [0.8.8] - 2023-07-30

### Changed

- Upgrade deps. Duckdb now compiles faster with less memory!

## [0.8.7] - 2023-07-30

### New

- Make duckdb optional

## [0.8.6] - 2023-06-21

### Fixed

- Make postgres index names shorter

## [0.8.5] - 2023-06-07

### Fixed

- Parquet field names incorrect

## [0.8.4] - 2023-05-26

### Changed

- Detect very large floats as strings.

## [0.8.3] - 2023-04-14

### Changed

- Fix postgres to use bigints as they are what is detected.

## [0.8.2] - 2023-04-12

### Changed

- Update dependencies.

## [0.8.1] - 2023-03-30

### Changed

- Only detect i64 and lower integers as most systems do not allow higher.

## [0.8.0] - 2023-01-22

### New

- Use duckdb for parquet converstion. More datetime formats allowed.
- `pipe` option to get data from stdin or named pipe.

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
