pub use crate::describer::Describer;
pub use crate::describe_csv::describe as describe_csv;
use std::path::PathBuf;
use pathdiff::diff_paths;
use thiserror::Error;
use serde_json::{json, Value};
use typed_builder::TypedBuilder;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Error, Debug)]
pub enum DescribeError {
    #[error("Could not write datapackage.json, check directory exists.")]
    WriteError(#[from] std::io::Error),
    #[error("Error writing datapackage.json.")]
    WriteJSONError(#[from] serde_json::Error),
    #[error("File {0} does not exist")]
    FileNotExist(String),
    #[error("Error Reading CSV file")]
    CSVRead(#[from] csv::Error),
}


#[derive(Default, Debug, TypedBuilder)]
pub struct Options {
    #[builder(default)]
    pub delimiter: Option<u8>,
    #[builder(default)]
    pub quote: Option<u8>,
    #[builder(default)]
    pub stats: bool,
    #[builder(default)]
    pub stats_csv: String,
}

fn simple_sniff(file: &PathBuf) -> Result<u8, DescribeError> {
    let file = File::open(file)?;
    let reader = BufReader::new(file);

    let mut top_10 = String::new();

    for line in reader.lines().take(10) {
        top_10.push_str(&line?)
    }

    let mut found = b',';

    for char in top_10.as_bytes() {
        if [b',', b'\t', b'|', b';', b':'].contains(char) {
            found = *char;
            break
        }
    }
    return Ok(found)
}

pub fn get_csv_reader(file: PathBuf, options: &Options) -> Result<(csv::Reader<std::fs::File>, u8, u8), DescribeError>{

    let mut delimiter = options.delimiter.unwrap_or(b',');
    let quote = options.quote.unwrap_or(b'"');

    if options.delimiter.is_none() {
        delimiter = simple_sniff(&file)?
    } 

    let mut reader_builder = csv::ReaderBuilder::new();

    reader_builder 
        .delimiter(delimiter)
        .quote(quote);

    Ok((reader_builder.from_reader(std::fs::File::open(&file)?), delimiter, quote))
}


pub fn describe_file(file: PathBuf, mut output_dir: PathBuf, options: &Options) -> Result<Value, DescribeError>{

    if !file.exists() {
        return Err(DescribeError::FileNotExist(file.to_string_lossy().into()))
    }

    if output_dir.to_string_lossy().is_empty() {
        output_dir.push(".");
    }

    let (csv_reader, delimiter, quote) = get_csv_reader(file.clone(), options)?;

    let mut describe_value = describe_csv(csv_reader, options.stats || !options.stats_csv.is_empty())?;

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

    if !options.stats_csv.is_empty() {
        datapackage_to_stats_csv(&datapackage, options.stats_csv.clone().into())?;
    }

    Ok(datapackage)
}


pub fn output_datapackage(files: Vec<PathBuf>, output_dir: PathBuf, options: &Options) -> Result<(), DescribeError> {
    let datapackage = describe_files(files, output_dir.clone(), options)?;
    let file = std::fs::File::create(output_dir.join("datapackage.json"))?;
    serde_json::to_writer_pretty(file, &datapackage)?;
    Ok(())
}

fn datapackage_to_stats_csv(datapackage: &Value, path: PathBuf) -> Result<(), DescribeError> {
    let resources_option = datapackage["resources"].as_array();

    let resources = resources_option.expect("we made the datapackage so key should be there");
    let core_fields = ["table", "field", "type", "format"];
    let stats_fields = [
        "min_len",
        "max_len",
        "min_str",
        "max_str",
        "count",
        "empty_count",
        "exact_unique",
        "estimate_unique",
        "sum",
        "mean",
        "variance",
        "stddev",
        "min_number",
        "max_number",
        "median",
        "lower_quartile",
        "upper_quartile",
        "deciles"
    ];
    let mut all_fields = core_fields.to_vec();
    all_fields.append(&mut stats_fields.to_vec());

    let mut writer = csv::Writer::from_path(path)?;
    writer.write_record(all_fields)?;

    for resource in resources {
        if let Some(fields) = resource["schema"]["fields"].as_array() {
            for field in fields {
                let mut row = vec![];
                row.push(resource["name"].as_str().unwrap_or("").to_string());
                row.push(field["name"].as_str().unwrap_or("").to_string());
                row.push(field["type"].as_str().unwrap_or("").to_string());
                row.push(field["format"].as_str().unwrap_or("").to_string());
                for stat in stats_fields {
                    let value = match field["stats"].get(stat).unwrap() {
                        Value::Null => {"".to_string()},
                        Value::String(a) => {a.to_owned()},
                        Value::Bool(a) => {a.to_string()},
                        Value::Number(a) => {a.to_string()},
                        Value::Array(a) => {serde_json::to_string(&a).expect("was json already")},
                        Value::Object(a) => {serde_json::to_string(&a).expect("was json already")}
                    };
                    row.push(value)
                }
                writer.write_record(row)?;
            }
        }
    }
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
        let options = Options::builder().stats_csv(path.join("stats.csv").to_string_lossy().into()).build();

        std::fs::copy("src/fixtures/all_types.csv", &input_file).unwrap();

        output_datapackage(vec![input_file], path.clone(), &options).unwrap();
        let reader = std::fs::File::open(path.join("datapackage.json")).unwrap();
        let value: serde_json::Value = serde_json::from_reader(reader).unwrap();
        insta::assert_yaml_snapshot!(value);

        let mut rows = vec![];

        let mut csv_reader = csv::Reader::from_path(path.join("stats.csv")).unwrap();

        rows.push(csv_reader.headers().unwrap().iter().map(|a| {a.to_string()}).collect());

        for row in csv_reader.deserialize() {
            let row: Vec<String> = row.unwrap();
            rows.push(row);
        }
        insta::assert_yaml_snapshot!(rows);
    }

    #[test]
    fn test_tab() {
        let options = Options::builder().build();
        let describe = describe_files(vec!["fixtures/tab_delimited.csv".into()], "".into(), &options).unwrap();
        insta::assert_yaml_snapshot!(describe);
    }

    #[test]
    fn test_semi_colon() {
        let options = Options::builder().build();
        let describe = describe_files(vec!["fixtures/semi_colon.csv".into()], "".into(), &options).unwrap();
        insta::assert_yaml_snapshot!(describe);
    }

    // #[test]
    // fn large_file_basic() {
    //     let options = Options::builder().stats_csv("moo.csv".into()).build();
    //     let describe = describe_files(vec!["rows_small.csv".into()], "".into(), &options).unwrap();
    //     insta::assert_yaml_snapshot!(describe);
    // }

}