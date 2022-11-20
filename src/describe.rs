pub use crate::describer::Describer;
pub use crate::describe_csv::describe as describe_csv;
use std::path::PathBuf;
use pathdiff::diff_paths;
use thiserror::Error;
use serde_json::{json, Value};


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



pub fn describe_files(files: Vec<PathBuf>, mut output_dir: PathBuf, delimeter: Option<u8>, quote: Option<u8>) -> Result<Value, DescribeError> {
    let mut resources = vec![];

    for file in files {
        if !file.exists() {
            return Err(DescribeError::FileNotExist(file.to_string_lossy().into()))
        }
        let mut sniffer = csv_sniffer::Sniffer::new();
        sniffer.header(csv_sniffer::metadata::Header {has_header_row: true, num_preamble_rows: 0});
        if let Some(delimeter) = delimeter {
            sniffer.delimiter(delimeter);
        }
        if let Some(quote) = quote {
            sniffer.quote(csv_sniffer::metadata::Quote::Some(quote));
        }

        let metadata = sniffer.sniff_reader(std::fs::File::open(&file)?);

        let csv_reader = if metadata.is_ok() {
            metadata.unwrap().dialect.open_reader(std::fs::File::open(&file)?)?
        } else {
            let mut reader_builder = csv::ReaderBuilder::new();

            reader_builder 
                .delimiter(delimeter.unwrap_or(b','))
                .quote(quote.unwrap_or(b'"'));
            reader_builder.from_reader(std::fs::File::open(&file)?)
        };

        
        let fields_value = describe_csv(csv_reader);

        if output_dir.to_string_lossy().is_empty() {
            output_dir.push(".");
        }

        let relative_path = diff_paths(
            std::fs::canonicalize(&file)?, 
            &std::fs::canonicalize(&output_dir)?
        );

        let file_name = file.file_name().expect("know file exists").to_string_lossy().into_owned();
        
        let file_no_extension = file_name.split(".").next();

        let resource = json!({
            "profile": "tabular-data-resource",
            "name": file_no_extension,
            "schema": {
                "fields": fields_value
            },
            "path": relative_path
        });
        resources.push(resource);
    }
    let datapackage = json!({
        "profile": "tabular-data-package",
        "resources": resources
    });

    Ok(datapackage)
}

pub fn output_datapackage(files: Vec<PathBuf>, output_dir: PathBuf, delimeter: Option<u8>, quote: Option<u8>) -> Result<(), DescribeError> {
    let datapackage = describe_files(files, output_dir.clone(), delimeter, quote)?;
    let file = std::fs::File::create(output_dir.join("datapackage.json"))?;
    serde_json::to_writer_pretty(file, &datapackage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{describe_files, output_datapackage};

    #[test]
    fn all_types() {
        let datapackage = describe_files(vec!["src/fixtures/all_types.csv".into()], "".into(), None, None).unwrap();
        insta::assert_yaml_snapshot!(datapackage);
        let datapackage = describe_files(vec!["src/fixtures/all_types.csv".into(), "src/fixtures/all_types.csv".into()], "src/fixtures".into(), None, None).unwrap();
        insta::assert_yaml_snapshot!(datapackage);
    }

    #[test]
    fn write_datapackage() {
        let tmpdir = tempdir::TempDir::new("").unwrap();
        let path = tmpdir.into_path();
        let input_file = path.join("all_types.csv");

        std::fs::copy("src/fixtures/all_types.csv", &input_file).unwrap();

        output_datapackage(vec![input_file], path.clone(), None, None).unwrap();
        let reader = std::fs::File::open(path.join("datapackage.json")).unwrap();
        let value: serde_json::Value = serde_json::from_reader(reader).unwrap();
        insta::assert_yaml_snapshot!(value);
    }
}