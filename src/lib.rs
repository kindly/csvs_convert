//! Converts CSV files into XLSX/SQLITE/POSTGRESQL/PARQUET fast.
//!
//! ## Aims

//! * Thorough type guessing of CSV columns, so there is no need to configure types of each field. Scans whole file first to make sure all types in a column are consistent. Can detect over 30 date/time formats as well as JSON data.
//! * Quick conversions/type guessing (uses rust underneath). Uses fast methods specific for each output format:
//!     * `copy` for postgres
//!     * Prepared statements for sqlite using c API.
//!     * Arrow reader for parquet
//!     * Write only mode for libxlsxwriter
//! * Tries to limit errors when inserting data into database by resorting to "text" if type guessing can't determine a more specific type.
//! * When inserting into existing databases automatically migrate schema of target to allow for new data (`evolve` option).
//! * Memory efficient. All csvs and outputs are streamed so all conversions should take up very little memory.
//! * Gather stats and information about CSV files into datapacakge.json file which can use it for customizing conversion.
//!
//! ## Drawbacks
//!
//! * CSV files currently need header rows.
//! * Whole file needs to be on disk as whole CSV is analyzed therefore files are read twice.

#[cfg(not(target_family = "wasm"))]
#[cfg(feature = "converters")]
mod converters;

mod describe;
mod describe_csv;
mod describer;

#[cfg(not(target_family = "wasm"))]
#[cfg(feature = "converters")]
mod zip_dir;

pub use describe::{
    describe_files, make_datapackage, output_datapackage, DescribeError, Options as DescribeOptions,
};
pub use describer::{Describer, Options as DescriberOptions};

#[cfg(feature = "converters")]
#[cfg(not(target_family = "wasm"))]
pub use converters::{
    csvs_to_postgres, csvs_to_postgres_with_options,
    csvs_to_sqlite, csvs_to_sqlite_with_options, csvs_to_xlsx, csvs_to_xlsx_with_options,
    csvs_to_ods, csvs_to_ods_with_options,
    datapackage_to_postgres,
    datapackage_to_postgres_with_options, datapackage_to_sqlite,
    datapackage_to_sqlite_with_options, datapackage_to_xlsx, datapackage_to_xlsx_with_options,
    datapackage_to_ods, datapackage_to_ods_with_options,
    merge_datapackage, merge_datapackage_jsons, merge_datapackage_with_options,
    Error, Options
};

#[cfg(feature = "parquet")]
pub use converters::{
    csvs_to_parquet, csvs_to_parquet_with_options, 
    datapackage_to_parquet, datapackage_to_parquet_with_options};

