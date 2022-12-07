pub use crate::describer::Describer;
pub use crate::describe_csv::describe as describe_csv;
use std::path::PathBuf;
use pathdiff::diff_paths;
use thiserror::Error;
use serde_json::{json, Value};
use typed_builder::TypedBuilder;

#[derive(Error, Debug)]
pub enum DescribeError {
    #[error("Error detecting csv file")]
    SnifferError(#[from] csv_sniffer::error::SnifferError),
    #[error("Could not write datapackage.json, check directory exists.")]
    WriteError(#[from] std::io::Error),
    #[error("Error writing datapackage.json.")]
    WriteJSONError(#[from] serde_json::Error),
    #[error("File {0} does not exist")]
    FileNotExist(String),
}


#[derive(Default, Debug, TypedBuilder)]
pub struct Options {
    #[builder(default)]
    pub delimiter: Option<u8>,
    #[builder(default)]
    pub quote: Option<u8>,
    #[builder(default)]
    pub stats: bool,
}


pub fn get_csv_reader(file: PathBuf, options: &Options) -> Result<(csv::Reader<std::fs::File>, u8, u8), DescribeError>{

    let mut sniffer = csv_sniffer::Sniffer::new();
    sniffer.header(csv_sniffer::metadata::Header {has_header_row: true, num_preamble_rows: 0});
    if let Some(delimeter) = options.delimiter {
        sniffer.delimiter(delimeter);
    }
    if let Some(quote) = options.quote {
        sniffer.quote(csv_sniffer::metadata::Quote::Some(quote));
    }

    let mut delimiter = options.delimiter.unwrap_or(b',');
    let mut quote = options.quote.unwrap_or(b'"');

    let metadata = sniffer.sniff_reader(std::fs::File::open(&file)?);

    if let Ok(meta) = metadata {
        delimiter = meta.dialect.delimiter;
        if let csv_sniffer::metadata::Quote::Some(sniffer_quote) = meta.dialect.quote { 
            quote = sniffer_quote;
        }
        Ok((meta.dialect.open_reader(std::fs::File::open(&file)?)?, delimiter, quote))
    } else {
        let mut reader_builder = csv::ReaderBuilder::new();

        reader_builder 
            .delimiter(delimiter)
            .quote(quote);

        Ok((reader_builder.from_reader(std::fs::File::open(&file)?), delimiter, quote))
    }

}


pub fn describe_file(file: PathBuf, mut output_dir: PathBuf, options: &Options) -> Result<Value, DescribeError>{

    if !file.exists() {
        return Err(DescribeError::FileNotExist(file.to_string_lossy().into()))
    }

    if output_dir.to_string_lossy().is_empty() {
        output_dir.push(".");
    }

    let (csv_reader, delimiter, quote) = get_csv_reader(file.clone(), options)?;

    let mut describe_value = describe_csv(csv_reader, options.stats);

    let fields_value = describe_value["fields"].take();

    let relative_path = diff_paths(
        std::fs::canonicalize(&file)?, 
        &std::fs::canonicalize(&output_dir)?
    );

    let file_name = file.file_name().expect("know file exists").to_string_lossy().into_owned();
    
    let file_no_extension = file_name.split(".").next();

    let delimiter = String::from_utf8_lossy(&[delimiter]).to_string();
    let quote = String::from_utf8_lossy(&[quote]).to_string();

    let resource = json!({
        "profile": "tabular-data-resource",
        "name": file_no_extension,
        "row_count": describe_value["row_count"],
        "schema": {
            "fields": fields_value
        },
        "path": relative_path,
        "dialect": {
            "delimiter": delimiter,
            "quoteChar": quote
        }
    });
    return Ok(resource)

}


pub fn describe_files(files: Vec<PathBuf>, output_dir: PathBuf, options: &Options) -> Result<Value, DescribeError> {
    let mut resources = vec![];

    for file in files {
        let resource = describe_file(file, output_dir.clone(), options)?;
        resources.push(resource);
    }
    let datapackage = json!({
        "profile": "tabular-data-package",
        "resources": resources
    });

    Ok(datapackage)
}


pub fn output_datapackage(files: Vec<PathBuf>, output_dir: PathBuf, options: &Options) -> Result<(), DescribeError> {
    let datapackage = describe_files(files, output_dir.clone(), options)?;
    let file = std::fs::File::create(output_dir.join("datapackage.json"))?;
    serde_json::to_writer_pretty(file, &datapackage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_types() {
        let options = Options::builder().build();
        let datapackage = describe_files(vec!["src/fixtures/all_types.csv".into()], "".into(), &options).unwrap();
        insta::assert_yaml_snapshot!(datapackage);
        let datapackage = describe_files(vec!["src/fixtures/all_types_semi_colon.csv".into()], "".into(), &options).unwrap();
        insta::assert_yaml_snapshot!(datapackage);
        let datapackage = describe_files(vec!["src/fixtures/all_types_semi_colon.csv".into(), "src/fixtures/all_types.csv".into()], "src/fixtures".into(), &options).unwrap();
        insta::assert_yaml_snapshot!(datapackage);
    }

    #[test]
    fn write_datapackage() {
        let tmpdir = tempdir::TempDir::new("").unwrap();
        let path = tmpdir.into_path();
        let input_file = path.join("all_types.csv");
        let options = Options::builder().build();

        std::fs::copy("src/fixtures/all_types.csv", &input_file).unwrap();

        output_datapackage(vec![input_file], path.clone(), &options).unwrap();
        let reader = std::fs::File::open(path.join("datapackage.json")).unwrap();
        let value: serde_json::Value = serde_json::from_reader(reader).unwrap();
        insta::assert_yaml_snapshot!(value);
    }

    // #[test]
    // fn large_file_basic() {
    //      let describe = describe_files(vec!["rows_small.csv".into()], "".into(),true, Some(b','), Some(b'"')).unwrap();
    //      insta::assert_yaml_snapshot!(describe);
    // }

}