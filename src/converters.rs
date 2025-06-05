use crate::describe;
use csv::ReaderBuilder;
use csv::Writer;
use minijinja::Environment;
use postgres::{Client, NoTls};
use rusqlite::Connection;
use spreadsheet_ods::OdsError;

use serde_json::{Value, json};
use snafu::prelude::*;
use snafu::{ensure, Snafu};
use std::collections::HashMap;
use std::fmt::Write as fmt_write;
use std::fs::{File, canonicalize};
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;
use typed_builder::TypedBuilder;
use rust_xlsxwriter::{Format, Workbook};
use rand::distr::{Alphanumeric, SampleString};

#[cfg(feature = "parquet")]
use arrow::csv::ReaderBuilder as ArrowReaderBuilder;
#[cfg(feature = "parquet")]
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
#[cfg(feature = "parquet")]
use arrow::error::ArrowError;
#[cfg(feature = "parquet")]
use parquet::{
    arrow::ArrowWriter, basic::Compression, errors::ParquetError,
    file::properties::WriterProperties,
};


#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{}", message))]
    DatapackageConvert { message: String },

    #[snafu(display("{}", message))]
    DatapackageMergeError { message: String },

    #[snafu(display("{}", message))]
    DatapackageXLSXError { message: String },

    #[snafu(display("{}", message))]
    DatapackageODSError { message: String },

    #[snafu(display("Error reading file {}: {}", filename, source))]
    IoError {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Error writing file {}: {}", filename, source))]
    WriteError {
        source: std::io::Error,
        filename: String,
    },

    #[snafu(display("Error parsing JSON {}: {}", filename, source))]
    JSONError {
        source: serde_json::Error,
        filename: String,
    },

    #[snafu(display("Error loading ZIP file {}: {}", filename, source))]
    ZipError {
        source: zip::result::ZipError,
        filename: String,
    },

    #[snafu(display("Error loading CSV file {}: {}", filename, source))]
    CSVError {
        source: csv::Error,
        filename: String,
    },

    #[snafu(display("Could not write row: {}", source))]
    CSVRowError { source: csv::Error },

    #[snafu(display("{}{}", message, source))]
    RusqliteError {
        source: rusqlite::Error,
        message: String,
    },

    #[snafu(display("{}", source))]
    JinjaError { source: minijinja::Error },

    #[snafu(display("Postgres Error: {}", source))]
    PostgresError { source: postgres::Error },

    #[snafu(display("Error with writing XLSX file"))]
    XLSXError { source: rust_xlsxwriter::XlsxError },

    #[snafu(display("Error with writing ODS file"))]
    OdsError { source: OdsError },

    #[snafu(display("Environment variable {} does not exist.", envvar))]
    EnvVarError {
        source: std::env::VarError,
        envvar: String,
    },

    #[snafu(display("Delimiter not valid utf-8"))]
    DelimeiterError { source: std::str::Utf8Error },

    #[snafu(display("{}", source))]
    JSONDecodeError { source: serde_json::Error },

    #[snafu(display("{}", source))]
    DescribeError { source: describe::DescribeError },

    #[cfg(feature = "parquet")]
    #[snafu(display("{}", source))]
    ParquetError { source: ParquetError },

    #[cfg(feature = "parquet")]
    #[snafu(display("{}", source))]
    ArrowError { source: ArrowError },
}

#[derive(Default, Debug, TypedBuilder, Clone)]
pub struct Options {
    #[builder(default)]
    pub delete_input_csv: bool,
    #[builder(default = "_".into())]
    pub seperator: String,
    #[builder(default)]
    pub use_titles: bool,
    #[builder(default)]
    pub drop: bool,
    #[builder(default)]
    pub schema: String,
    #[builder(default)]
    pub evolve: bool,
    #[builder(default)]
    pub delimiter: Option<u8>,
    #[builder(default)]
    pub quote: Option<u8>,
    #[builder(default = true)]
    pub double_quote: bool,
    #[builder(default=None)]
    pub escape: Option<u8>,
    #[builder(default=None)]
    pub comment: Option<u8>,
    #[builder(default)]
    pub stats: bool,
    #[builder(default)]
    pub datapackage_string: bool,
    #[builder(default)]
    pub stats_csv: String,
    #[builder(default)]
    pub threads: usize,
    #[builder(default)]
    pub dump_file: String,
    #[builder(default)]
    pub pipe: bool,
    #[builder(default)]
    pub truncate: bool,
    #[builder(default)]
    pub all_strings: bool,
}

lazy_static::lazy_static! {
    #[allow(clippy::invalid_regex)]
    pub static ref INVALID_REGEX: regex::Regex = regex::RegexBuilder::new(r"[\000-\010]|[\013-\014]|[\016-\037]")
        .octal(true)
        .build()
        .expect("we know the regex is fine");
}

fn make_mergeable_resource(mut resource: Value) -> Result<Value, Error> {
    let mut fields = resource["schema"]["fields"].take();
    let fields_option = fields.as_array_mut();

    ensure!(
        fields_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `fields` list"
        }
    );

    let mut new_fields = serde_json::Map::new();
    for field in fields_option.expect("we checked above").drain(..) {
        let name_option = field["name"].as_str();
        ensure!(
            name_option.is_some(),
            DatapackageMergeSnafu {
                message: "Each field needs a name"
            }
        );
        new_fields.insert(name_option.expect("we checked above").to_owned(), field);
    }

    resource["schema"]
        .as_object_mut()
        .expect("we know its an obj")
        .insert("fields".to_string(), new_fields.into());

    Ok(resource)
}

fn make_mergeable_datapackage(mut value: Value) -> Result<Value, Error> {
    let mut resources = value["resources"].take();

    let resources_option = resources.as_array_mut();
    ensure!(
        resources_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `resources` key as an array"
        }
    );

    let mut new_resources = serde_json::Map::new();
    for resource in resources_option.expect("checked above").drain(..) {
        let path;
        {
            let path_str = resource["path"].as_str();
            ensure!(
                path_str.is_some(),
                DatapackageMergeSnafu {
                    message: "datapackage resource needs a name or path"
                }
            );
            path = path_str.expect("we checked above").to_owned();
        }

        let new_resource = make_mergeable_resource(resource)?;
        new_resources.insert(path, new_resource);
    }

    value
        .as_object_mut()
        .expect("we know its an obj")
        .insert("resources".into(), new_resources.into());

    Ok(value)
}

fn make_datapackage_from_mergeable(mut value: Value) -> Result<Value, Error> {
    let mut resources = value["resources"].take();

    let resources_option = resources.as_object_mut();

    let mut new_resources = vec![];
    for resource in resources_option.expect("we know its an obj").values_mut() {
        let new_resource = make_resource_from_mergable(resource.clone())?;
        new_resources.push(new_resource);
    }

    value
        .as_object_mut()
        .expect("we know its an obj")
        .insert("resources".into(), new_resources.into());

    Ok(value)
}

fn make_resource_from_mergable(mut resource: Value) -> Result<Value, Error> {
    let mut fields = resource["schema"]["fields"].take();
    let fields_option = fields.as_object_mut();

    let mut new_fields = vec![];
    for field in fields_option.expect("we know its an obj").values_mut() {
        new_fields.push(field.clone());
    }

    resource["schema"]
        .as_object_mut()
        .expect("we know its an obj")
        .insert("fields".to_string(), new_fields.into());

    Ok(resource)
}

fn datapackage_json_to_value(filename: &str) -> Result<Value, Error> {
    if filename.ends_with(".json") {
        let file = File::open(filename).context(IoSnafu { filename })?;
        let json: Value =
            serde_json::from_reader(BufReader::new(file)).context(JSONSnafu { filename })?;
        Ok(json)
    } else if filename.ends_with(".zip") {
        let file = File::open(filename).context(IoSnafu { filename })?;
        let mut zip = zip::ZipArchive::new(file).context(ZipSnafu { filename })?;
        let zipped_file = zip
            .by_name("datapackage.json")
            .context(ZipSnafu { filename })?;
        let json: Value =
            serde_json::from_reader(BufReader::new(zipped_file)).context(JSONSnafu { filename })?;
        Ok(json)
    } else if PathBuf::from(filename).is_dir() {
        let mut path = PathBuf::from(filename);
        path.push("datapackage.json");
        let file = File::open(path).context(IoSnafu { filename })?;
        let json: Value =
            serde_json::from_reader(BufReader::new(file)).context(JSONSnafu { filename })?;
        Ok(json)
    } else {
        Err(Error::DatapackageMergeError {
            message: "could not detect a datapackage".into(),
        })
    }
}

fn merge_datapackage_json(mut base: Value, mut merger: Value) -> Result<Value, Error> {
    let merger_resources_value = merger["resources"].take();

    let merger_resources = merger_resources_value
        .as_object()
        .expect("we know its an obj");
    let base_resources = base["resources"]
        .as_object_mut()
        .expect("we know its an obj");

    for (resource, resource_value) in merger_resources {
        if !base_resources.contains_key(resource) {
            base_resources.insert(resource.clone(), resource_value.clone());
        } else {
            for (field, field_value) in resource_value["schema"]["fields"]
                .as_object()
                .expect("we know its an obj")
            {
                ensure!(
                    field_value.is_object(),
                    DatapackageMergeSnafu {
                        message: "Each field needs to be an object"
                    }
                );

                let base_fields = base_resources[resource]["schema"]["fields"]
                    .as_object_mut()
                    .expect("we know its an obj");

                if !base_fields.contains_key(field) {
                    base_fields.insert(field.clone(), field_value.clone());
                } else {
                    ensure!(
                        base_fields[field].is_object(),
                        DatapackageMergeSnafu {
                            message: "Each field needs to be an object"
                        }
                    );
                    let base_fieldinfo = base_fields[field]
                        .as_object_mut()
                        .expect("we know its an obj");

                    let base_type = base_fieldinfo["type"].as_str().unwrap_or_default();
                    let field_type = field_value["type"].as_str().unwrap_or_default();

                    if field_type != base_type || base_type.is_empty() || field_type.is_empty() {
                        base_fieldinfo.insert("type".to_string(), "string".into());
                    }

                    let base_count = base_fieldinfo["count"].as_u64().unwrap_or_default();
                    let field_count = field_value["count"].as_u64().unwrap_or_default();

                    if base_count > 0 && field_count > 0 {
                        base_fieldinfo
                            .insert("count".to_string(), (field_count + base_count).into());
                    }
                }
            }
        }
    }
    Ok(base)
}

pub fn merge_datapackage_jsons(datapackages: Vec<String>) -> Result<Value, Error> {
    ensure!(
        datapackages.len() > 1,
        DatapackageMergeSnafu {
            message: "Need more 2 or more datapackages"
        }
    );
    let mut merged_value =
        make_mergeable_datapackage(datapackage_json_to_value(&datapackages[0])?)?;

    for file in datapackages[1..].iter() {
        merged_value = merge_datapackage_json(
            merged_value,
            make_mergeable_datapackage(datapackage_json_to_value(file)?)?,
        )?;
    }

    make_datapackage_from_mergeable(merged_value)
}

fn write_merged_csv(
    csv_reader: csv::Reader<impl std::io::Read>,
    mut csv_writer: Writer<File>,
    resource_fields: &HashMap<String, usize>,
    output_fields: &[String],
) -> Result<Writer<File>, Error> {
    let output_map: Vec<Option<usize>> = output_fields
        .iter()
        .map(|field| resource_fields.get(field).copied())
        .collect();
    let output_map_len = output_map.len();
    for row in csv_reader.into_records() {
        let mut output_row = Vec::with_capacity(output_map_len);
        let row = row.context(CSVRowSnafu {})?;
        for item in &output_map {
            match item {
                Some(index) => output_row.push(row.get(*index).expect("index should exist")),
                None => output_row.push(""),
            }
        }
        csv_writer
            .write_record(output_row)
            .context(CSVRowSnafu {})?;
    }
    Ok(csv_writer)
}


fn get_path(file: &str, resource_path: &str, options: &Options) -> Result<PathBuf, Error> {
    if options.datapackage_string {
        Ok(resource_path.into())
    } else if file.ends_with(".json") {
        let mut file_pathbuf = PathBuf::from(file);
        file_pathbuf.pop();
        file_pathbuf.push(resource_path);
        Ok(file_pathbuf)
    //} else if file.ends_with(".zip") {
    //    let zip_file = File::open(file).context(IoSnafu { filename: file })?;
    //    let zip = zip::ZipArchive::new(zip_file).context(ZipSnafu { filename: file })?;
    //    Ok(Readers::Zip(zip))
    } else if PathBuf::from(&file).is_dir() {
        let file_pathbuf = PathBuf::from(file);
        let file_pathbuf = file_pathbuf.join(resource_path);
        Ok(file_pathbuf.clone())
    } else {
        Err(Error::DatapackageMergeError {
            message: "could not detect a datapackage".into(),
        })
    }
}

pub fn merge_datapackage(output_path: PathBuf, datapackages: Vec<String>) -> Result<(), Error> {
    let options = Options::builder().build();
    merge_datapackage_with_options(output_path, datapackages, options)
}

pub fn merge_datapackage_with_options(
    mut output_path: PathBuf,
    datapackages: Vec<String>,
    options: Options,
) -> Result<(), Error> {
    ensure!(
        datapackages.len() > 1,
        DatapackageMergeSnafu {
            message: "Need more 2 or more files"
        }
    );

    let original_path = output_path.clone();

    let mut tmpdir_option = None;

    if let Some(extension) = output_path.extension() {
        if extension == "zip" {
            output_path.pop();
            let tmpdir = TempDir::new_in(&output_path).context(IoSnafu {
                filename: output_path.to_string_lossy(),
            })?;
            output_path = tmpdir.path().to_owned();
            tmpdir_option = Some(tmpdir)
        }
    }

    std::fs::create_dir_all(&output_path).context(IoSnafu {
        filename: output_path.to_string_lossy(),
    })?;

    let mut merged_datapackage_json = merge_datapackage_jsons(datapackages.clone())?;

    let path = PathBuf::from(&output_path);

    let datapackage_json_path_buf = path.join("datapackage.json");

    let writer = File::create(&datapackage_json_path_buf).context(IoSnafu {
        filename: datapackage_json_path_buf.to_string_lossy(),
    })?;

    serde_json::to_writer_pretty(writer, &merged_datapackage_json).context(JSONSnafu {
        filename: datapackage_json_path_buf.to_string_lossy(),
    })?;

    let mut csv_outputs = HashMap::new();
    let mut output_fields = HashMap::new();

    for resource in merged_datapackage_json["resources"]
        .as_array_mut()
        .expect("we know its an array")
    {
        let mut field_order_map = serde_json::Map::new();
        let mut fields: Vec<String> = Vec::new();
        for (index, field) in resource["schema"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            let name = field["name"].as_str().expect("we know its a string");
            field_order_map.insert(name.into(), index.into());
            fields.push(name.to_owned());
        }

        let resource_path = resource["path"]
            .as_str()
            .expect("we know its a string")
            .to_owned();

        let mut full_path = path.join(&resource_path);
        full_path.pop();
        std::fs::create_dir_all(&full_path).context(IoSnafu {
            filename: full_path.to_string_lossy(),
        })?;

        let mut writer = Writer::from_path(path.join(&resource_path)).context(CSVSnafu {
            filename: &resource_path,
        })?;
        writer.write_record(fields.clone()).context(CSVSnafu {
            filename: &resource_path,
        })?;
        csv_outputs.insert(resource_path.clone(), writer);

        output_fields.insert(resource_path.clone(), fields);

        resource
            .as_object_mut()
            .expect("we know its a obj")
            .insert("field_order_map".into(), field_order_map.into());
    }

    for file in datapackages.iter() {
        let mut datapackage_json = datapackage_json_to_value(file)?;
        for resource in datapackage_json["resources"].as_array_mut().unwrap() {
            let mut resource_fields = HashMap::new();
            for (num, field) in resource["schema"]["fields"]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
            {
                resource_fields.insert(field["name"].as_str().unwrap().to_owned(), num);
            }
            let resource_path = resource["path"].as_str().unwrap().to_owned();

            let mut csv_output = csv_outputs.remove(&resource_path).unwrap();

            let output_fields = output_fields.get_mut(&resource_path).unwrap();

            let tempdir: Option<TempDir>;

            let csv_path = if file.ends_with(".zip") {
                tempdir = Some(TempDir::new().context(IoSnafu { filename: file })?);
                extract_csv_file(file, &resource_path, &tempdir)?
            } else {
                get_path(file, &resource_path, &options)?
            };

            let csv_reader =
                get_csv_reader_builder(&options, resource).from_path(&csv_path).unwrap();

            csv_output =
                write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields)?;

            if options.delete_input_csv {
                std::fs::remove_file(&csv_path).context(IoSnafu {
                    filename: csv_path.to_string_lossy(),
                })?;
            }

            csv_outputs.insert(resource_path, csv_output);
        }
    }

    for (name, csv_file) in csv_outputs.iter_mut() {
        csv_file.flush().context(IoSnafu { filename: name })?;
    }

    if tmpdir_option.is_some() {
        crate::zip_dir::zip_dir(&output_path, &original_path).context(ZipSnafu {
            filename: original_path.to_string_lossy(),
        })?;
    }

    Ok(())
}

fn extract_csv_file(file: &String, resource_path: &String, tempdir: &Option<TempDir>) -> Result<PathBuf, Error> {
    let zip_file = File::open(file).context(IoSnafu { filename: file })?;
    let mut zip = zip::ZipArchive::new(zip_file).context(ZipSnafu { filename: file })?;
    let mut zipped_file = zip
        .by_name(resource_path)
        .context(ZipSnafu { filename: file })?;
    let output_path = tempdir.as_ref().unwrap().path().join("file.csv");
    let mut output_file = File::create(&output_path).context(IoSnafu { filename: file })?;
    std::io::copy(&mut zipped_file, &mut output_file).context(IoSnafu { filename: file })?;
    Ok(output_path)
}

fn get_csv_reader_builder(options: &Options, resource: &Value) -> csv::ReaderBuilder {
    let mut reader_builder = ReaderBuilder::new();
    let mut delimiter = options.delimiter.unwrap_or(b',');
    if let Some(dialect_delimiter) = resource["dialect"]["delimiter"].as_str() {
        if dialect_delimiter.as_bytes().len() == 1 {
            delimiter = *dialect_delimiter.as_bytes().first().unwrap()
        }
    };

    let mut quote = options.quote.unwrap_or(b'"');
    if let Some(dialect_quote) = resource["dialect"]["quoteChar"].as_str() {
        if dialect_quote.as_bytes().len() == 1 {
            quote = *dialect_quote.as_bytes().first().unwrap()
        }
    };

    let mut double_quote = options.double_quote;
    if let Some(dialect_double_quote) = resource["dialect"]["doubleQuote"].as_bool() {
        double_quote = dialect_double_quote
    };

    reader_builder
        .delimiter(delimiter)
        .quote(quote)
        .double_quote(double_quote)
        .escape(options.escape)
        .comment(options.comment);

    reader_builder
}

fn rand() -> String {
    return Alphanumeric.sample_string(&mut rand::rng(), 5);
}

fn to_db_type(type_: String, format: String) -> String {
    match type_.as_str() {
        "string" => "TEXT".to_string(),
        "date" => {
            if POSTGRES_ALLOWED_DATE_FORMATS.contains(&format.as_str()) || format.is_empty() {
                "TIMESTAMP".to_string()
            } else {
                "TEXT".into()
            }
        }
        "datetime" => {
            if POSTGRES_ALLOWED_DATE_FORMATS.contains(&format.as_str()) || format.is_empty() {
                "TIMESTAMP".to_string()
            } else {
                "TEXT".into()
            }
        }
        "datetime_tz" => {
            if POSTGRES_ALLOWED_DATE_FORMATS.contains(&format.as_str()) || format.is_empty() {
                "TIMESTAMP".to_string()
            } else {
                "TEXT".into()
            }
        }
        "number" => "NUMERIC".to_string(),
        "object" => "JSONB".to_string(),
        "array" => "JSONB".to_string(),
        "integer" => "BIGINT".to_string(),
        "boolean" => "BOOL".to_string(),
        _ => "TEXT".to_string(),
    }
}

fn clean_field(_state: &minijinja::State, field: String) -> Result<String, minijinja::Error> {
    if INVALID_REGEX.is_match(&field) {
        return Ok(INVALID_REGEX.replace_all(&field, " ").to_string());
    }
    Ok(field)
}

fn render_sqlite_table(value: Value) -> Result<String, Error> {
    let sqlite_table = r#"
    CREATE TABLE [{{title|default(name)}}] (
        {% for field in schema.fields %}
           {% if not loop.first %}, {% endif %}[{{field.name}}] {{ to_db_type(field.type, field.format) }} #nl
        {% endfor %}
        {% if schema.primaryKey is string %}
           , PRIMARY KEY ([{{schema.primaryKey}}]) #nl
        {% endif %}
        {% if schema.primaryKey is sequence %}
           , PRIMARY KEY ([{{schema.primaryKey | join("],[")}}]) #nl
        {% endif %}
        {% if schema.foreignKeys is sequence %}
           {% for foreignKey in schema.foreignKeys %}
              {% if foreignKey.fields is string %}
                , FOREIGN KEY ([{{foreignKey.fields}}]) REFERENCES [{{foreignKey.reference.resource}}]([{{foreignKey.reference.fields}}]) #nl {% endif %} {% if foreignKey.fields is sequence %} , FOREIGN KEY ([{{foreignKey.fields | join("],[")}}])
                  REFERENCES [{{foreignKey.reference.resource}}]([{{foreignKey.reference.fields | join("],[")}}]) #nl
              {% endif %}
           {% endfor %}
        {% endif %}
    ); #nl

    {% if schema.foreignKeys is sequence %}
        {% for foreignKey in schema.foreignKeys %}
            {% if foreignKey.fields is string %}
              CREATE INDEX [idx_{{name}}_{{foreignKey.fields}}] ON [{{title|default(name)}}] ([{{foreignKey.fields}}]); #nl
            {% endif %}
            {% if foreignKey.fields is sequence %}
              CREATE INDEX [idx_{{name}}_{{foreignKey.fields | join("_")}}] ON [{{title|default(name)}}] ([{{foreignKey.fields | join("],[")}}]); #nl
            {% endif %}
        {% endfor %}
    {% endif %}

    "#;
    let sqlite_table = sqlite_table.replace("  ", "");
    let sqlite_table = sqlite_table.replace('\n', "");
    let sqlite_table = sqlite_table.replace("#nl", "\n");

    let mut env = Environment::new();
    env.add_function("to_db_type", to_db_type);
    env.add_template("sqlite_resource", &sqlite_table).unwrap();
    let tmpl = env.get_template("sqlite_resource").unwrap();
    tmpl.render(value).context(JinjaSnafu {})
}

fn render_postgres_table(value: Value) -> Result<String, Error> {
    let postgres_table = r#"
    CREATE TABLE IF NOT EXISTS "{{title|default(name)}}" (
        {% for field in schema.fields %}
           {% if not loop.first %}, {% endif %}"{{field.name|clean_field}}" {{to_db_type(field.type, field.format)}} #nl
        {% endfor %}
        {% if schema.primaryKey is string %}
           , PRIMARY KEY ("{{schema.primaryKey}}") #nl
        {% endif %}
        {% if schema.primaryKey is sequence %}
           , PRIMARY KEY ("{{schema.primaryKey | join('","')}}") #nl
        {% endif %}
        {% if schema.foreignKeys is sequence %}
           {% for foreignKey in schema.foreignKeys %}
              {% if foreignKey.fields is string %}
                , FOREIGN KEY ("{{foreignKey.fields}}") REFERENCES "{{foreignKey.reference.resource}}"("{{foreignKey.reference.fields}}") #nl
              {% endif %}
              {% if foreignKey.fields is sequence %}
                , FOREIGN KEY ("{{foreignKey.fields | join('","')}}")
                  REFERENCES "{{foreignKey.reference.resource}}"("{{foreignKey.reference.fields | join('","')}}") #nl
              {% endif %}
           {% endfor %}
        {% endif %}
    ); #nl

    {% if schema.foreignKeys is sequence %}
        {% for foreignKey in schema.foreignKeys %}
            {% if foreignKey.fields is string %}
              CREATE INDEX "idx_{{rand()}}_{{foreignKey.fields}}" ON "{{title|default(name)}}" ("{{foreignKey.fields}}"); #nl
            {% endif %}
            {% if foreignKey.fields is sequence %}
              CREATE INDEX "idx_{{rand()}}_{{foreignKey.fields | join("_")}}" ON "{{title|default(name)}}" ("{{foreignKey.fields | join('","')}}"); #nl
            {% endif %}
        {% endfor %}
    {% endif %}

    "#;
    let postgres_table = postgres_table.replace("  ", "");
    let postgres_table = postgres_table.replace('\n', "");
    let postgres_table = postgres_table.replace("#nl", "\n");

    let mut env = Environment::new();
    env.add_function("to_db_type", to_db_type);
    env.add_function("rand", rand);
    env.add_filter("clean_field", clean_field);
    env.add_template("postgres_resource", &postgres_table)
        .unwrap();
    let tmpl = env.get_template("postgres_resource").unwrap();
    tmpl.render(value).context(JinjaSnafu {})
}

lazy_static::lazy_static! {
    pub static ref POSTGRES_ALLOWED_DATE_FORMATS: Vec<&'static str> =
    vec!(
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %I:%M:%S %P",
        "%Y-%m-%d %I:%M %P",
        "%Y %b %d %H:%M:%S",
        "%B %d %Y %H:%M:%S",
        "%B %d %Y %I:%M:%S %P",
        "%B %d %Y %I:%M %P",
        "%Y %b %d at %I:%M %P",
        "%d %B %Y %H:%M:%S",
        "%d %B %Y %H:%M",
        "%d %B %Y %H:%M:%S.%f",
        "%d %B %Y %I:%M:%S %P",
        "%d %B %Y %I:%M %P",
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S.%f%#z",
        "%Y-%m-%d %H:%M%#z",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%y %H:%M:%S.%f",
        "%m/%d/%y %I:%M:%S %P",
        "%m/%d/%y %I:%M %P",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S.%f",
        "%m/%d/%Y %I:%M:%S %P",
        "%m/%d/%Y %I:%M %P",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y/%m/%d %I:%M:%S %P",
        "%Y/%m/%d %I:%M %P",
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S.%f %Z",
        "%B %d %Y %H:%M:%S %Z",
        "%B %d %Y %H:%M %Z",
        "%B %d %Y %I:%M:%S %P %Z",
        "%B %d %Y %I:%M %P %Z",
        "rfc2822",
        "rfc3339",
        "%Y-%m-%d",
        "%Y-%b-%d",
        "%B %d %Y %H:%M",
        "%B %d %y",
        "%B %d %Y",
        "%d %B %y",
        "%d %B %Y",
        "%m/%d/%y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%m.%d.%Y",
        "%Y.%m.%d",
        "%y%m%d %H:%M:%S");


    pub static ref PARQUET_ALLOWED_DEFAULT: Vec<&'static str> =
    vec!(
        "rfc3339",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S"
        );


    // pub static ref PARQUET_ALLOWED_FORMAT: Vec<&'static str> =
    // vec!(
    //     //"%Y-%m-%d %H:%M:%S",
    //     "%Y-%m-%d %H:%M",
    //     "%Y-%m-%d %I:%M:%S %p",
    //     "%Y-%m-%d %I:%M %p",
    //     "%Y %b %d %H:%M:%S",
    //     "%B %d %Y %H:%M:%S",
    //     "%B %d %Y %I:%M:%S %p",
    //     "%B %d %Y %I:%M %p",
    //     "%Y %b %d at %I:%M %p",
    //     "%d %B %Y %H:%M:%p",
    //     "%d %B %Y %H:%M",
    //     "%d %B %Y %I:%M:%S %p",
    //     "%d %B %Y %I:%M %p",
    //     "%B %d %Y %H:%M",
    //     "%m/%d/%y %H:%M:%S",
    //     "%m/%d/%y %H:%M",
    //     "%m/%d/%y %I:%M:%S %p",
    //     "%m/%d/%y %I:%M %p",
    //     "%m/%d/%Y %H:%M:%S",
    //     "%m/%d/%Y %H:%M",
    //     "%m/%d/%Y %I:%M:%S %p",
    //     "%m/%d/%Y %I:%M %p",
    //     "%d/%m/%y %H:%M:%S",
    //     "%d/%m/%y %H:%M",
    //     "%d/%m/%y %I:%M:%S %p",
    //     "%d/%m/%y %I:%M %p",
    //     "%d/%m/%Y %H:%M:%S",
    //     "%d/%m/%Y %H:%M",
    //     "%d/%m/%Y %I:%M:%S %p",
    //     "%d/%m/%Y %I:%M %p",
    //     "%Y/%m/%d %H:%M:%S",
    //     "%Y/%m/%d %H:%M",
    //     "%Y/%m/%d %I:%M:%S %p",
    //     "%Y/%m/%d %I:%M %p",
    //     "%y%m%d %H:%M:%S",
    // );

}

fn insert_sql_data(
    csv_reader: csv::Reader<impl std::io::Read>,
    conn: &mut rusqlite::Connection,
    resource: Value,
) -> Result<(), Error> {
    let tx = conn.transaction().context(RusqliteSnafu {
        message: "Error making transaction: ",
    })?;

    let mut table = resource["name"].as_str().unwrap();

    if let Some(title) = resource["title"].as_str() {
        table = title
    }

    let mut fields_len = 0;
    let mut fields = vec![];

    if let Some(fields_vec) = resource["schema"]["fields"].as_array() {
        fields_len = fields_vec.len();
        for field_value in fields_vec {
            if let Some(field) = field_value["name"].as_str() {
                fields.push(format!("[{field}]"))
            }
        }
    };

    let fields = fields.join(", ");

    let mut question_marks = "?,".repeat(fields_len);

    question_marks.pop();

    {
        let mut statement = tx
            .prepare_cached(&format!(
                "INSERT INTO [{table}]({fields}) VALUES ({question_marks})"
            ))
            .context(RusqliteSnafu {
                message: "Error preparing sqlite statment: ",
            })?;

        for row in csv_reader.into_deserialize() {
            let this_row: Vec<String> = row.context(CSVSnafu { filename: table })?;

            statement
                .execute(rusqlite::params_from_iter(this_row.iter()))
                .context(RusqliteSnafu {
                    message: "Error inserting data to sqlite: ",
                })?;
        }
    }
    tx.commit().context(RusqliteSnafu {
        message: "Error commiting sqlite: ",
    })?;
    Ok(())
}

pub fn csvs_to_sqlite(db_path: String, csvs: Vec<PathBuf>) -> Result<Value, Error> {
    let describe_options = describe::Options::builder().build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    let mut options = Options::builder().build();
    options.datapackage_string = true;
    datapackage_to_sqlite_with_options(
        db_path,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn csvs_to_sqlite_with_options(
    db_path: String,
    csvs: Vec<PathBuf>,
    mut options: Options,
) -> Result<Value, Error> {
    let describe_options = describe::Options::builder()
        .threads(options.threads)
        .stats(options.stats)
        .stats_csv(options.stats_csv.clone())
        .delimiter(options.delimiter)
        .quote(options.quote)
        .all_strings(options.all_strings)
        .build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;

    datapackage_to_sqlite_with_options(
        db_path,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn datapackage_to_sqlite(db_path: String, datapackage: String) -> Result<(), Error> {
    let options = Options::builder().build();
    datapackage_to_sqlite_with_options(db_path, datapackage, options)
}

pub fn datapackage_to_sqlite_with_options(
    db_path: String,
    datapackage: String,
    options: Options,
) -> Result<(), Error> {
    let (table_to_schema, ordered_tables) = get_table_info(&datapackage, &options)?;

    let mut conn= if !db_path.is_empty() {
        Some(Connection::open(&db_path).context(RusqliteSnafu {
            message: "Error opening connection: ",
        })?)
    } else {
        None
    };

    let mut dump_writer: Option<Box<dyn Write>> =  if !options.dump_file.is_empty() {
        if options.dump_file == "-" {
            Some(Box::new(std::io::stdout()))
        } else {
            Some(Box::new(File::create(&options.dump_file).context(WriteSnafu {filename: db_path})?))
        }
    } else {
        None
    };

    let pragmas = "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = 0;
         PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA temp_store = MEMORY;";

    if let Some(conn) = conn.as_mut() {
        conn.execute_batch(
            pragmas
        )
        .context(RusqliteSnafu {
            message: "Error executing pragmas: ",
        })?;
    };


    if let Some(dump_writer) = dump_writer.as_mut() {
        writeln!(dump_writer, "{pragmas}").context(IoSnafu {filename: &options.dump_file})?;
        writeln!(dump_writer, ".mode csv").context(IoSnafu {filename: &options.dump_file})?;
    }

    for table in ordered_tables {
        let resource = table_to_schema.get(&table).unwrap();


        let mut existing_columns: HashMap<String, String> = HashMap::new();

        if options.drop || options.evolve || options.truncate {
            if let Some(conn) = conn.as_mut() {
                let mut fields_query = conn
                    .prepare("select name, type from pragma_table_info(?)")
                    .context(RusqliteSnafu {
                        message: "Error peparing sql",
                    })?;

                let mut rows = fields_query.query([&table]).context(RusqliteSnafu {
                    message: "Error peparing sql",
                })?;

                while let Some(row) = rows.next().context(RusqliteSnafu {
                    message: "Error fetching rows",
                })? {
                    existing_columns.insert(
                        row.get(0).context(RusqliteSnafu {
                            message: "Error fetching rows",
                        })?,
                        row.get(1).context(RusqliteSnafu {
                            message: "Error fetching rows",
                        })?,
                    );
                }
            };
        }

        if !existing_columns.is_empty() && options.truncate {
            if let Some(conn) = conn.as_mut() {
                conn.execute(&format!("DELETE FROM [{table}];"), [])
                    .context(RusqliteSnafu {
                        message: "Error making sqlite tables: ",
                    })?;
                }

        }

        let mut create = false;

        if existing_columns.is_empty() {
            create = true
        } 

        if options.drop {
            if let Some(conn) = conn.as_mut() {
                conn.execute(&format!("drop table if exists [{table}];"), [])
                    .context(RusqliteSnafu {
                        message: "Error making sqlite tables: ",
                    })?;
                create = true
            }
            if let Some(dump_writer) = dump_writer.as_mut() {
                writeln!(dump_writer, "drop table if exists [{table}];").context(IoSnafu {filename: &options.dump_file})?;
            }
        }

        ensure!(
            resource["path"].is_string(),
            DatapackageMergeSnafu {
                message: "Datapackages resources need a `path`"
            }
        );

        if create {
            let resource_sqlite = render_sqlite_table(resource.clone())?;
            if let Some(conn) = conn.as_mut() {
                conn.execute(&resource_sqlite, []).context(RusqliteSnafu {
                    message: "Error making sqlite tables: ",
                })?;
            }
            if let Some(dump_writer) = dump_writer.as_mut() {
                writeln!(dump_writer, "{}", &resource_sqlite).context(IoSnafu {filename: &options.dump_file})?;
            }
        } else if options.evolve {
            let (add_columns, _alter_columns) = get_column_changes(resource, existing_columns);
            for (name, type_) in add_columns {
                if let Some(conn) = conn.as_mut() {
                    conn.execute(&format!("ALTER TABLE {table} ADD [{name}] {type_}"), [])
                        .context(RusqliteSnafu {
                            message: "Error altering sqlite tables: ",
                        })?;
                }
                if let Some(dump_writer) = dump_writer.as_mut() {
                    writeln!(dump_writer, "ALTER TABLE {table} ADD [{name}] {type_}").context(IoSnafu {filename: &options.dump_file})?;
                }
            }
        }

        let resource_path = resource["path"].as_str().unwrap();

        if let Some(conn) = conn.as_mut() {

            let tempdir: Option<TempDir>;

            let csv_path = if datapackage.ends_with(".zip") {
                tempdir = Some(TempDir::new().context(IoSnafu { filename: &datapackage })?);
                extract_csv_file(&datapackage.to_string(), &resource_path.to_owned(), &tempdir)?
            } else {
                get_path(&datapackage, resource_path, &options)?
            };

            let csv_reader =
                get_csv_reader_builder(&options, resource).from_path(&csv_path).context(CSVSnafu { filename: csv_path.to_string_lossy().to_owned() })?;
            insert_sql_data(csv_reader, conn, resource.clone())?;

            if options.delete_input_csv {
                std::fs::remove_file(&csv_path).context(IoSnafu {
                    filename: csv_path.to_string_lossy(),
                })?;
            }
        }

        if let Some(dump_writer) = dump_writer.as_mut() {
            let table_value = json!(table);
            let table_name = resource.get("title").unwrap_or(resource.get("name").unwrap_or(&table_value)).as_str().unwrap_or(&table);

            let mut delimiter_u8 = options.delimiter.unwrap_or(b',');

            if let Some(dialect_delimiter) = resource["dialect"]["delimiter"].as_str() {
                if dialect_delimiter.as_bytes().len() == 1 {
                    delimiter_u8 = *dialect_delimiter.as_bytes().first().unwrap()
                }
            };

            let delimiter = std::str::from_utf8(&[delimiter_u8])
                .context(DelimeiterSnafu {})?
                .to_owned();

            writeln!(dump_writer, ".separator '{delimiter}'").context(IoSnafu {filename: &options.dump_file})?;
            writeln!(dump_writer, ".import '{resource_path}' {table_name} --skip 1 ").context(IoSnafu {filename: &options.dump_file})?;
        }

    }

    Ok(())
}

fn get_table_info(
    datapackage: &str,
    options: &Options,
) -> Result<(HashMap<String, Value>, Vec<String>), Error> {
    let mut datapackage_value = if options.datapackage_string {
        serde_json::from_str(datapackage).context(JSONDecodeSnafu {})?
    } else {
        datapackage_json_to_value(datapackage)?
    };

    let resources_option = datapackage_value["resources"].as_array_mut();
    ensure!(
        resources_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `resources` key as an array"
        }
    );
    let mut table_links = Vec::new();
    let mut table_to_schema = HashMap::new();
    for resource in resources_option.unwrap().drain(..) {
        let table_name = if let Some(name) = resource["title"].as_str() {
            name
        } else if let Some(name) = resource["name"].as_str() {
            name
        } else {
            ""
        };

        if !table_name.is_empty() {
            if let Some(foreign_keys) = resource["schema"]["foreignKeys"].as_array() {
                for value in foreign_keys {
                    if let Some(foreign_key_table) = value["reference"]["resource"].as_str() {
                        table_links.push((foreign_key_table.to_owned(), table_name.to_owned()));
                    }
                }
            }
            table_links.push((table_name.to_owned(), table_name.to_owned()));
            table_to_schema.insert(table_name.to_owned(), resource.clone());
        }
    }
    let mut relationhip_graph = petgraph::graphmap::DiGraphMap::<_, _, std::hash::RandomState>::new();
    for (x, y) in table_links.iter() {
        relationhip_graph.add_edge(y, x, 1);
    }
    let ordered_tables = petgraph::algo::kosaraju_scc(&relationhip_graph);
    let tables: Vec<String> = ordered_tables
        .into_iter()
        .flatten()
        .map(|x| x.to_owned())
        .collect();
    Ok((table_to_schema, tables))
}


#[cfg(feature = "parquet")]
fn create_parquet(
    file: PathBuf,
    resource: Value,
    mut output_path: PathBuf,
    options: &Options,
) -> Result<(), Error> {
    ensure!(
        resource["name"].is_string(),
        DatapackageMergeSnafu {
            message: "Datapackage resource needs a name"
        }
    );

    output_path.push(format!("{}.parquet", resource["name"].as_str().unwrap()));

    let mut arrow_fields = vec![];

    ensure!(
        resource["schema"]["fields"].is_array(),
        DatapackageMergeSnafu {
            message: "Datapackage resource needs a `fields` list."
        }
    );

    let fields = resource["schema"]["fields"].as_array().unwrap();
    for field in fields {
        ensure!(
            field.is_object(),
            DatapackageMergeSnafu {
                message: "Datapackage field needs to be an object"
            }
        );
        ensure!(
            field["name"].is_string(),
            DatapackageMergeSnafu {
                message: "Datapackage field needs a name"
            }
        );
        ensure!(
            field["type"].is_string(),
            DatapackageMergeSnafu {
                message: "Datapackage field needs a type"
            }
        );

        let name = field["name"].as_str().unwrap();
        let field_type = field["type"].as_str().unwrap();

        let format_type = field["format"].as_str().unwrap_or("_");

        let field = match (field_type, format_type) {
            ("number", _) => Field::new(name, DataType::Float64, true),
            ("integer", _) => Field::new(name, DataType::Int64, true),
            ("boolean", _) => Field::new(name, DataType::Boolean, true),
            ("datetime", f) => {
                if PARQUET_ALLOWED_DEFAULT.contains(&f) {
                    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
                } else {
                    Field::new(name, DataType::Utf8, true)
                }
            },
            _ => Field::new(name, DataType::Utf8, true),
        };
        arrow_fields.push(field);
    }

    let mut delimiter = options.delimiter.unwrap_or(b',');
    if let Some(dialect_delimiter) = resource["dialect"]["delimiter"].as_str() {
        if dialect_delimiter.as_bytes().len() == 1 {
            delimiter = *dialect_delimiter.as_bytes().first().unwrap()
        }
    };

    let file = File::open(file.clone()).context(IoSnafu { filename: file.to_string_lossy().to_string() })?;

    let arrow_csv_reader = ArrowReaderBuilder::new(std::sync::Arc::new(Schema::new(arrow_fields)))
        .with_header(true)
        .with_delimiter(delimiter)
        .with_batch_size(1024).build(file).context(ArrowSnafu {})?;

    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::SNAPPY);

    let output = File::create(&output_path).context(IoSnafu {
        filename: output_path.to_string_lossy(),
    })?;

    let mut writer = ArrowWriter::try_new(output, arrow_csv_reader.schema(), Some(props.build()))
        .context(ParquetSnafu {})?;

    for batch in arrow_csv_reader {
        let record_batch = batch.context(ArrowSnafu {})?;
        writer.write(&record_batch).context(ParquetSnafu {})?;
    }

    match writer.close() {
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    }
    .context(ParquetSnafu {})?;

    Ok(())
}

#[cfg(feature = "parquet")]
pub fn csvs_to_parquet(output_path: String, csvs: Vec<PathBuf>) -> Result<Value, Error> {
    let mut options = Options::builder().build();
    let describe_options = describe::Options::builder().build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_parquet_with_options(
        PathBuf::from(output_path),
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

#[cfg(feature = "parquet")]
pub fn csvs_to_parquet_with_options(
    output_path: String,
    csvs: Vec<PathBuf>,
    mut options: Options,
) -> Result<Value, Error> {
    let describe_options = describe::Options::builder()
        .threads(options.threads)
        .stats(options.stats)
        .stats_csv(options.stats_csv.clone())
        .delimiter(options.delimiter)
        .quote(options.quote)
        .all_strings(options.all_strings)
        .build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_parquet_with_options(
        PathBuf::from(output_path),
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

#[cfg(feature = "parquet")]
pub fn datapackage_to_parquet(output_path: PathBuf, datapackage: String) -> Result<(), Error> {
    let options = Options::builder().build();
    datapackage_to_parquet_with_options(output_path, datapackage, options)
}

#[cfg(feature = "parquet")]
pub fn datapackage_to_parquet_with_options(
    output_path: PathBuf,
    datapackage: String,
    options: Options,
) -> Result<(), Error> {
    std::fs::create_dir_all(&output_path).context(IoSnafu {
        filename: output_path.to_string_lossy(),
    })?;

    let mut datapackage_value = if options.datapackage_string {
        serde_json::from_str(&datapackage).context(JSONDecodeSnafu {})?
    } else {
        datapackage_json_to_value(&datapackage)?
    };

    let resources_option = datapackage_value["resources"].as_array_mut();
    ensure!(
        resources_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `resources` key as an array"
        }
    );

    for resource in resources_option.unwrap() {
        let resource_path = resource["path"].as_str().unwrap();

        let tempdir: Option<TempDir>;

        let csv_path = if datapackage.ends_with(".zip") {
            tempdir = Some(TempDir::new().context(IoSnafu { filename: &datapackage })?);
            extract_csv_file(&datapackage.to_string(), &resource_path.to_owned(), &tempdir)?
        } else {
            get_path(&datapackage, resource_path, &options)?
        };

        create_parquet(csv_path.clone(), resource.clone(), output_path.clone(), &options)?;

        if options.delete_input_csv {
            std::fs::remove_file(&csv_path).context(IoSnafu {
                filename: csv_path.to_string_lossy(),
            })?;
        }
    }

    Ok(())
}

pub fn truncate_xlsx_title(mut title: String, seperator: &str) -> String {
    let parts: Vec<&str> = title.split(seperator).collect();
    if parts.len() == 1 || title.len() <= 31 {
        title.truncate(31);
        return title;
    }

    let mut last_part = parts.last().unwrap().to_string();

    let length_of_last_part = parts.last().unwrap().len();

    let rest = 31 - std::cmp::min(length_of_last_part, 31);

    let max_len_of_part_with_sep = rest / (parts.len() - 1);

    let len_of_part =
        max_len_of_part_with_sep - std::cmp::min(max_len_of_part_with_sep, seperator.len());

    if len_of_part < 1 {
        last_part.truncate(31);
        return last_part;
    }
    let mut new_parts: Vec<String> = vec![];
    for part in parts[..parts.len() - 1].iter() {
        let end_new_part = std::cmp::min(len_of_part, part.len());
        let new_part = part[..end_new_part].to_string();
        new_parts.push(new_part);
    }
    new_parts.push(last_part);

    new_parts.join(seperator)
}

fn create_sheet(
    csv_reader: csv::Reader<impl std::io::Read>,
    resource: Value,
    workbook: &mut Workbook,
    options: &Options,
) -> Result<(), Error> {
    let bold_format = Format::new().set_bold();

    let base_format = Format::new();

    let mut field_types = vec![];
    if let Some(fields_vec) = resource["schema"]["fields"].as_array() {
        for value in fields_vec {
            if let Some(field_type) = value["type"].as_str() {
                field_types.push(field_type.to_owned());
            }
        }
    };

    ensure!(
        field_types.len() < 65536,
        DatapackageXLSXSnafu {
            message: "Too many columns for XLSX file"
        }
    );

    let mut title = String::new();

    if let Some(t) = resource["name"].as_str() {
        title = t.into()
    };

    if let Some(t) = resource["title"].as_str() {
        if options.use_titles {
            title = t.into()
        }
    }

    ensure!(
        !title.is_empty(),
        DatapackageXLSXSnafu {
            message: "A data resource either needs a name or title."
        }
    );

    let new_title = truncate_xlsx_title(title.clone(), &options.seperator);

    let worksheet = workbook.add_worksheet_with_low_memory();
    worksheet.set_name(&new_title).context(XLSXSnafu {})?;


    for (row_num, row) in csv_reader.into_records().enumerate() {
        let this_row = row.context(CSVSnafu { filename: &title })?;

        let mut format = &base_format;

        ensure!(
            row_num < 1048575,
            DatapackageXLSXSnafu {
                message: "Number of rows is too large for XLSX file"
            }
        );

        if row_num == 0 {
            ensure!(
                this_row.len() == field_types.len(),
                DatapackageXLSXSnafu {
                    message: "Number of fields in datapackage needs to match CSV fields."
                }
            );
            format = &bold_format;
        }

        for (col_index, value) in this_row.iter().enumerate() {
            let mut cell = value.to_string();

            if ["number", "integer"].contains(&field_types[col_index].as_str()) {
                if let Ok(number) = value.parse::<f64>() {
                    if number.is_finite() {
                        worksheet
                            .write_number(
                                row_num.try_into().unwrap(),
                                col_index.try_into().unwrap(),
                                number,
                            )
                            .context(XLSXSnafu {})?;
                    } else {
                        log::warn!("Skipping number \"{number}\" as it is not allowed in XLSX format");
                    }
                    continue;
                }
            }

            if INVALID_REGEX.is_match(&cell) {
                cell = INVALID_REGEX.replace_all(&cell, "").to_string();
            }

            if cell.len() > 32767 {
                log::warn!("WARNING: Cell larger than 32767 chararcters which is too large for XLSX format. The cell will be truncated, so some data will be missing.");
                let mut index: usize = 32767;
                while !cell.is_char_boundary(index) {
                    index -= 1;
                }
                cell.truncate(index)
            }

            worksheet
                .write_with_format(
                    row_num.try_into().expect("already tested length of string"),
                    col_index.try_into().expect("already checked field count"),
                    &cell,
                    format,
                )
                .context(XLSXSnafu {})?;
        }
    }
    Ok(())
}

pub fn csvs_to_xlsx(xlsx_path: String, csvs: Vec<PathBuf>) -> Result<Value, Error> {
    let mut options = Options::builder().build();
    let describe_options = describe::Options::builder().build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_xlsx_with_options(
        xlsx_path,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn csvs_to_xlsx_with_options(
    xlsx_path: String,
    csvs: Vec<PathBuf>,
    mut options: Options,
) -> Result<Value, Error> {
    let describe_options = describe::Options::builder()
        .threads(options.threads)
        .stats(options.stats)
        .stats_csv(options.stats_csv.clone())
        .delimiter(options.delimiter)
        .quote(options.quote)
        .all_strings(options.all_strings)
        .build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_xlsx_with_options(
        xlsx_path,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn datapackage_to_xlsx(xlsx_path: String, datapackage: String) -> Result<(), Error> {
    let options = Options::builder().build();
    datapackage_to_xlsx_with_options(xlsx_path, datapackage, options)
}

pub fn datapackage_to_xlsx_with_options(
    xlsx_path: String,
    datapackage: String,
    options: Options,
) -> Result<(), Error> {
    let mut datapackage_value = if options.datapackage_string {
        serde_json::from_str(&datapackage).context(JSONDecodeSnafu {})?
    } else {
        datapackage_json_to_value(&datapackage)?
    };

    let resources_option = datapackage_value["resources"].as_array_mut();
    ensure!(
        resources_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `resources` key as an array"
        }
    );

    let mut pathbuf = PathBuf::from(&xlsx_path);
    pathbuf.pop();

    let mut workbook = Workbook::new();
    workbook.set_tempdir(pathbuf).context(XLSXSnafu {})?;

    for resource in resources_option.unwrap() {
        let resource_path = resource["path"].as_str().unwrap();

        let tempdir: Option<TempDir>;

        let csv_path = if datapackage.ends_with(".zip") {
            tempdir = Some(TempDir::new().context(IoSnafu { filename: &datapackage })?);
            extract_csv_file(&datapackage.to_string(), &resource_path.to_owned(), &tempdir)?
        } else {
            get_path(&datapackage, resource_path, &options)?
        };

        let csv_reader = get_csv_reader_builder(&options, resource)
            .has_headers(false)
            .from_path(&csv_path).context(CSVSnafu {filename: csv_path.to_string_lossy().to_string()})?;

        if options.delete_input_csv {
            std::fs::remove_file(&csv_path).context(IoSnafu {
                filename: csv_path.to_string_lossy(),
            })?;
        }
        create_sheet(csv_reader, resource.clone(), &mut workbook, &options)?;
    }

    workbook.save(&xlsx_path).context(XLSXSnafu {})?;

    Ok(())
}

pub fn csvs_to_postgres(postgres_url: String, csvs: Vec<PathBuf>) -> Result<Value, Error> {
    let mut options = Options::builder().build();
    let describe_options = describe::Options::builder().build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_postgres_with_options(
        postgres_url,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn csvs_to_postgres_with_options(
    postgres_url: String,
    csvs: Vec<PathBuf>,
    mut options: Options,
) -> Result<Value, Error> {
    let describe_options = describe::Options::builder()
        .threads(options.threads)
        .stats(options.stats)
        .stats_csv(options.stats_csv.clone())
        .delimiter(options.delimiter)
        .quote(options.quote)
        .all_strings(options.all_strings)
        .build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_postgres_with_options(
        postgres_url,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn datapackage_to_postgres(postgres_url: String, datapackage: String) -> Result<(), Error> {
    let options = Options::builder().build();
    datapackage_to_postgres_with_options(postgres_url, datapackage, options)
}


pub fn datapackage_to_postgres_with_options(
    postgres_url: String,
    datapackage: String,
    options: Options,
) -> Result<(), Error> {
    let (table_to_schema, ordered_tables) = get_table_info(&datapackage, &options)?;

    let mut conf = postgres_url.clone();

    if postgres_url.trim_start().to_lowercase().starts_with("env") {
        let split: Vec<_> = postgres_url.split('=').into_iter().collect();
        let env = if split.len() == 1 {
            "DATABASE_URL"
        } else if split.len() == 2 {
            split[1].trim()
        } else {
            ""
        };
        if !env.is_empty() {
            conf = std::env::var(env).context(EnvVarSnafu {
                envvar: env.to_owned(),
            })?;
        }
    }

    let mut client= if !postgres_url.is_empty() {
        Some(Client::connect(&conf, NoTls).context(PostgresSnafu {})?)
    } else {
        None
    };

    let mut dump_writer: Option<Box<dyn Write>> =  if !options.dump_file.is_empty() {
        if options.dump_file == "-" {
            Some(Box::new(std::io::stdout()))
        } else {
            Some(Box::new(File::create(&options.dump_file).context(WriteSnafu {filename: postgres_url})?))
        }
    } else {
        None
    };

    for table in ordered_tables {
        let resource = table_to_schema.get(&table).unwrap();

        ensure!(
            resource["path"].is_string(),
            DatapackageMergeSnafu {
                message: "Datapackages resources need a `path`"
            }
        );

        let resource_path = resource["path"].as_str().unwrap();

        let mut resource_postgres = render_postgres_table(resource.clone())?;

        let mut schema_table = format!("\"{table}\"");

        if !options.schema.is_empty() {
            resource_postgres = format!(
r#"
CREATE SCHEMA IF NOT EXISTS "{schema}";
set search_path = "{schema}";
{resource_postgres};
"#,
                schema = options.schema
            );
            schema_table = format!("\"{schema}\".\"{table}\"", schema = options.schema);
        }

        let mut create = true;

        if let Some(client) = client.as_mut() {
            let result = client
                .query_one("SELECT to_regclass($1)::TEXT", &[&schema_table])
                .context(PostgresSnafu {})?;
            let exists: Option<String> = result.get(0);
            create = exists.is_none();
        }


        let mut drop = options.drop;

        let mut existing_columns = None;

        if !create && options.evolve {
            if let Some(client) = client.as_mut() {
                let result = client
                    .query_opt(&format!("SELECT * FROM {schema_table} limit 1"), &[])
                    .context(PostgresSnafu {})?;
                if result.is_none() {
                    drop = true
                }
                if let Some(row) = result {
                    let mut columns = HashMap::new();
                    for column in row.columns() {
                        columns.insert(column.name().to_owned(), column.type_().to_string());
                    }
                    existing_columns = Some(columns)
                }
            }
        }

        if options.truncate && !create {
            if let Some(client) = client.as_mut() {
                client
                    .batch_execute(&format!("TRUNCATE TABLE {schema_table} CASCADE;"))
                    .context(PostgresSnafu {})?;
            }
        }

        if drop && !create {
            create = true;
            let mut drop_statement = String::new();
            if !options.schema.is_empty() {
                write!(
                    drop_statement,
                    "set search_path = \"{schema}\";",
                    schema = options.schema
                )
                .unwrap();
            }
            write!(drop_statement, "DROP TABLE IF EXISTS \"{table}\" CASCADE;").unwrap();
            if let Some(client) = client.as_mut() {
                if let Some(dump_writer) = dump_writer.as_mut() {
                    writeln!(dump_writer, "{drop_statement}").context(IoSnafu {filename: &options.dump_file})?;
                }
                client
                    .batch_execute(&drop_statement)
                    .context(PostgresSnafu {})?
            }
        }

        if create {
            if let Some(dump_writer) = dump_writer.as_mut() {
                writeln!(dump_writer, "{resource_postgres}").context(IoSnafu {filename: &options.dump_file})?;
            }
            if let Some(client) = client.as_mut() {
                client
                    .batch_execute(&resource_postgres)
                    .context(PostgresSnafu {})?;
            }
        }

        let mut columns = vec![];
        if let Some(fields) = resource["schema"]["fields"].as_array() {
            for field in fields {
                if let Some(name) = field["name"].as_str() {
                    columns.push(format!("\"{name}\""));
                }
            }
        }
        let mut all_columns = columns.join(", ");

        if INVALID_REGEX.is_match(&all_columns) {
            all_columns = INVALID_REGEX.replace_all(&all_columns, "").to_string();
        }

        if let Some(existing_columns) = existing_columns {
            let (add_columns, alter_columns) = get_column_changes(resource, existing_columns);
            for (name, type_) in add_columns {
                if let Some(client) = client.as_mut() {
                    if let Some(dump_writer) = dump_writer.as_mut() {
                        writeln!(dump_writer, "ALTER TABLE {schema_table} ADD COLUMN \"{name}\" {type_}").context(IoSnafu {filename: &options.dump_file})?;
                    }
                    client
                        .batch_execute(&format!(
                            "ALTER TABLE {schema_table} ADD COLUMN \"{name}\" {type_}"
                        ))
                        .context(PostgresSnafu {})?;
                }
            }

            for name in alter_columns {
                if let Some(client) = client.as_mut() {
                    if let Some(dump_writer) = dump_writer.as_mut() {
                        writeln!(dump_writer, "ALTER TABLE {schema_table} ALTER COLUMN \"{name}\" TYPE TEXT").context(IoSnafu {filename: &options.dump_file})?;
                    }
                    client
                        .batch_execute(&format!(
                            "ALTER TABLE {schema_table} ALTER COLUMN \"{name}\" TYPE TEXT"
                        ))
                        .context(PostgresSnafu {})?;
                }
            }
        }


        let mut delimiter_u8 = options.delimiter.unwrap_or(b',');

        if let Some(dialect_delimiter) = resource["dialect"]["delimiter"].as_str() {
            if dialect_delimiter.as_bytes().len() == 1 {
                delimiter_u8 = *dialect_delimiter.as_bytes().first().unwrap()
            }
        };

        let delimiter = std::str::from_utf8(&[delimiter_u8])
            .context(DelimeiterSnafu {})?
            .to_owned();

        let mut quote_u8 = options.quote.unwrap_or(b'"');
        if let Some(dialect_quote) = resource["dialect"]["quote"].as_str() {
            if dialect_quote.as_bytes().len() == 1 {
                quote_u8 = *dialect_quote.as_bytes().first().unwrap()
            }
        };

        let quote = std::str::from_utf8(&[quote_u8])
            .context(DelimeiterSnafu {})?
            .to_owned();

        let query = format!("copy {schema_table}({all_columns}) from STDIN WITH (FORMAT CSV, HEADER, QUOTE '{quote}', DELIMITER '{delimiter}', FORCE_NULL ({all_columns}))");

        if let Some(dump_writer) = dump_writer.as_mut() {
            let full_path = canonicalize(resource_path).context(IoSnafu {filename: resource_path})?;
            let full_path = full_path.to_string_lossy();
            writeln!(dump_writer, "\\copy {schema_table}({all_columns}) from '{full_path}' WITH (FORMAT CSV, HEADER, QUOTE '{quote}', DELIMITER '{delimiter}', FORCE_NULL ({all_columns}))").context(IoSnafu {filename: &options.dump_file})?;
        }

        let tempdir: Option<TempDir>;

        let csv_path = if datapackage.ends_with(".zip") {
            tempdir = Some(TempDir::new().context(IoSnafu { filename: &datapackage })?);
            extract_csv_file(&datapackage.to_string(), &resource_path.to_owned(), &tempdir)?
        } else {
            get_path(&datapackage, resource_path, &options)?
        };

        if let Some(client) = client.as_mut() {
            let mut file = std::fs::File::open(&csv_path).context(IoSnafu {
                filename: csv_path.to_string_lossy().to_owned(),
            })?;
            let mut writer = client.copy_in(&query).context(PostgresSnafu {})?;
            std::io::copy(&mut file, &mut writer).context(IoSnafu {
                filename: csv_path.to_string_lossy().to_owned(),
            })?;
            file.flush().unwrap();
            writer.finish().context(PostgresSnafu {})?;

            if options.delete_input_csv {
                std::fs::remove_file(&csv_path).context(IoSnafu {
                    filename: csv_path.to_string_lossy(),
                })?;
            }
        }
    }

    Ok(())
}

fn get_column_changes(
    resource: &Value,
    existing_columns: HashMap<String, String>,
) -> (Vec<(String, String)>, Vec<String>) {
    let mut add_columns = vec![];
    let mut alter_columns = vec![];
    if let Some(fields) = resource["schema"]["fields"].as_array() {
        for field in fields {
            if let Some(name) = field["name"].as_str() {
                if let Some(type_) = field["type"].as_str() {
                    let existing_column_type = existing_columns.get(name);
                    if let Some(existing_column_type) = existing_column_type {
                        if to_db_type(
                            type_.to_string(),
                            field["format"].as_str().unwrap_or("").into(),
                        )
                        .to_lowercase()
                            != existing_column_type.to_lowercase()
                        {
                            alter_columns.push(name.to_owned());
                        }
                    } else {
                        add_columns.push((
                            name.to_owned(),
                            to_db_type(
                                type_.to_string(),
                                field["format"].as_str().unwrap_or("").into(),
                            ),
                        ))
                    }
                }
            }
        }
    }
    (add_columns, alter_columns)
}


fn create_ods_sheet(
    csv_reader: csv::Reader<impl std::io::Read>,
    resource: Value,
    workbook: &mut spreadsheet_ods::WorkBook,
    options: &Options,
) -> Result<(), Error> {
    let mut bold_format = spreadsheet_ods::CellStyle::new_empty();
    bold_format.set_font_bold();
    let bold_format_ref = workbook.add_cellstyle(bold_format);

    let base_format = spreadsheet_ods::CellStyle::new_empty();
    let base_format_ref = workbook.add_cellstyle(base_format);


    let mut field_types = vec![];
    if let Some(fields_vec) = resource["schema"]["fields"].as_array() {
        for value in fields_vec {
            if let Some(field_type) = value["type"].as_str() {
                field_types.push(field_type.to_owned());
            }
        }
    };

    ensure!(
        field_types.len() < 65536,
        DatapackageODSSnafu {
            message: "Too many columns for ods file"
        }
    );

    let mut title = String::new();

    if let Some(t) = resource["name"].as_str() {
        title = t.into()
    };

    if let Some(t) = resource["title"].as_str() {
        if options.use_titles {
            title = t.into()
        }
    }

    ensure!(
        !title.is_empty(),
        DatapackageODSSnafu {
            message: "A data resource either needs a name or title."
        }
    );

    let new_title = truncate_xlsx_title(title.clone(), &options.seperator);

    let mut worksheet = spreadsheet_ods::Sheet::new(new_title);

    for (row_num, row) in csv_reader.into_records().enumerate() {
        let this_row = row.context(CSVSnafu { filename: &title })?;

        let mut format = base_format_ref.clone();

        ensure!(
            row_num < 1048575,
            DatapackageODSSnafu {
                message: "Number of rows is too large for ods file"
            }
        );

        if row_num == 0 {
            ensure!(
                this_row.len() == field_types.len(),
                DatapackageODSSnafu {
                    message: "Number of fields in datapackage needs to match CSV fields."
                }
            );
            format = bold_format_ref.clone();
        }

        for (col_index, value) in this_row.iter().enumerate() {
            let mut cell = value.to_string();

            if ["number", "integer"].contains(&field_types[col_index].as_str()) {
                if let Ok(number) = value.parse::<f64>() {
                    if number.is_finite() {
                        worksheet.set_value(row_num.try_into().unwrap(), col_index.try_into().unwrap(), number);
                    } else {
                        log::warn!("Skipping number \"{number}\" as it is not allowed in ods format");
                    }
                    continue;
                }
            }

            if INVALID_REGEX.is_match(&cell) {
                cell = INVALID_REGEX.replace_all(&cell, "").to_string();
            }

            if cell.len() > 32767 {
                log::warn!("WARNING: Cell larger than 32767 chararcters which is too large for ods format. The cell will be truncated, so some data will be missing.");
                let mut index: usize = 32767;
                while !cell.is_char_boundary(index) {
                    index -= 1;
                }
                cell.truncate(index)
            }

            worksheet
                .set_styled_value(
                    row_num.try_into().expect("already tested length of string"),
                    col_index.try_into().expect("already checked field count"),
                    &cell,
                    &format,
                );
        }
    }

    workbook.push_sheet(worksheet);
    Ok(())
}

pub fn csvs_to_ods(ods_path: String, csvs: Vec<PathBuf>) -> Result<Value, Error> {
    let mut options = Options::builder().build();
    let describe_options = describe::Options::builder().build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_ods_with_options(
        ods_path,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn csvs_to_ods_with_options(
    ods_path: String,
    csvs: Vec<PathBuf>,
    mut options: Options,
) -> Result<Value, Error> {
    let describe_options = describe::Options::builder()
        .threads(options.threads)
        .stats(options.stats)
        .stats_csv(options.stats_csv.clone())
        .delimiter(options.delimiter)
        .quote(options.quote)
        .all_strings(options.all_strings)
        .build();
    let datapackage = describe::describe_files(csvs, PathBuf::new(), &describe_options)
        .context(DescribeSnafu {})?;
    options.datapackage_string = true;
    datapackage_to_ods_with_options(
        ods_path,
        serde_json::to_string(&datapackage).expect("should serialize"),
        options,
    )?;
    Ok(datapackage)
}

pub fn datapackage_to_ods(ods_path: String, datapackage: String) -> Result<(), Error> {
    let options = Options::builder().build();
    datapackage_to_ods_with_options(ods_path, datapackage, options)
}

pub fn datapackage_to_ods_with_options(
    ods_path: String,
    datapackage: String,
    options: Options,
) -> Result<(), Error> {
    let mut datapackage_value = if options.datapackage_string {
        serde_json::from_str(&datapackage).context(JSONDecodeSnafu {})?
    } else {
        datapackage_json_to_value(&datapackage)?
    };

    let resources_option = datapackage_value["resources"].as_array_mut();
    ensure!(
        resources_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `resources` key as an array"
        }
    );

    let mut pathbuf = PathBuf::from(&ods_path);
    pathbuf.pop();

    let mut workbook = spreadsheet_ods::WorkBook::new_empty();

    for resource in resources_option.unwrap() {
        let resource_path = resource["path"].as_str().unwrap();

        let tempdir: Option<TempDir>;

        let csv_path = if datapackage.ends_with(".zip") {
            tempdir = Some(TempDir::new().context(IoSnafu { filename: &datapackage })?);
            extract_csv_file(&datapackage.to_string(), &resource_path.to_owned(), &tempdir)?
        } else {
            get_path(&datapackage, resource_path, &options)?
        };

        let csv_reader = get_csv_reader_builder(&options, resource)
            .has_headers(false)
            .from_path(&csv_path).context(CSVSnafu {filename: csv_path.to_string_lossy().to_string()})?;

        if options.delete_input_csv {
            std::fs::remove_file(&csv_path).context(IoSnafu {
                filename: csv_path.to_string_lossy(),
            })?;
        }
        create_ods_sheet(csv_reader, resource.clone(), &mut workbook, &options)?;
    }

    spreadsheet_ods::write_ods(&mut workbook, &ods_path).context(OdsSnafu {})?;

    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;

    use rusqlite::types::ValueRef;
    use std::io::BufRead;
    use parquet::file::reader::SerializedFileReader;

    fn test_merged_csv_output(tmp: &PathBuf, name: String) {
        let csv_dir = tmp.join("csv");
        let paths = std::fs::read_dir(csv_dir).unwrap();
        for path in paths {
            let path = path.unwrap().path();
            let file_name = path.file_name().unwrap().to_string_lossy().into_owned();
            let test_name = format!("{name}_{file_name}");
            let file = File::open(path).unwrap();
            let lines: Vec<String> = std::io::BufReader::new(file)
                .lines()
                .map(|x| x.unwrap())
                .collect();
            insta::assert_yaml_snapshot!(test_name, lines);
        }
    }

    fn test_datapackage_merge(name: &str, datapackage1: &str, datapackage2: &str) {
        {
            let tmp_dir = TempDir::new().unwrap();
            let tmp = tmp_dir.path().to_owned();

            let options = Options::builder().build();

            merge_datapackage_with_options(
                tmp.clone(),
                vec![
                    format!("fixtures/{datapackage1}/datapackage.json"),
                    format!("fixtures/{datapackage2}/datapackage.json"),
                ],
                options,
            )
            .unwrap();

            insta::assert_yaml_snapshot!(
                format!("{name}_json"),
                datapackage_json_to_value(&tmp.to_string_lossy()).unwrap()
            );
            test_merged_csv_output(&tmp, format!("{name}_json"))
        }

        {
            let temp_dir = TempDir::new().unwrap();
            let tmp = temp_dir.path().to_path_buf();

            merge_datapackage(
                tmp.clone(),
                vec![
                    format!("fixtures/{datapackage1}"),
                    format!("fixtures/{datapackage2}"),
                ],
            )
            .unwrap();

            insta::assert_yaml_snapshot!(
                format!("{name}_folder"),
                datapackage_json_to_value(&tmp.to_string_lossy()).unwrap()
            );
            test_merged_csv_output(&tmp, format!("{name}_folder"))
        }

        {
            let temp_dir = TempDir::new().unwrap();
            let tmp = temp_dir.path().to_path_buf();

            merge_datapackage(
                tmp.clone(),
                vec![
                    format!("fixtures/{datapackage1}.zip"),
                    format!("fixtures/{datapackage2}.zip"),
                ],
            )
            .unwrap();

            insta::assert_yaml_snapshot!(
                format!("{name}_zip"),
                datapackage_json_to_value(&tmp.to_string_lossy()).unwrap()
            );
            test_merged_csv_output(&tmp, format!("{name}_zip"))
        }
    }

    #[test]
    fn test_datapackage_merge_self() {
        test_datapackage_merge("base", "base_datapackage", "base_datapackage");
    }

    #[test]
    fn test_datapackage_add_resource() {
        test_datapackage_merge("add_resource", "base_datapackage", "add_resource");
    }

    #[test]
    fn test_datapackage_add_different_resource() {
        test_datapackage_merge(
            "add_different_resource",
            "base_datapackage",
            "add_different_resource",
        );
    }

    #[test]
    fn test_datapackage_add_field() {
        test_datapackage_merge("add_field", "base_datapackage", "add_field");
    }

    #[test]
    fn test_conflict_types() {
        test_datapackage_merge("conflict_types", "base_datapackage", "conflict_types");
    }

    #[test]
    fn test_sqlite() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        let options = Options::builder().delete_input_csv(true).build();

        std::fs::copy(
            "fixtures/add_resource/datapackage.json",
            tmp.join("datapackage.json"),
        )
        .unwrap();
        std::fs::create_dir_all(tmp.join("csv")).unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games.csv",
            tmp.join("csv/games.csv"),
        )
        .unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games2.csv",
            tmp.join("csv/games2.csv"),
        )
        .unwrap();

        datapackage_to_sqlite_with_options(
            tmp.join("sqlite.db").to_string_lossy().into(),
            tmp.to_string_lossy().into(),
            options,
        )
        .unwrap();

        assert!(tmp.join("sqlite.db").exists());
        assert!(!tmp.join("csv/games.csv").exists());
        assert!(!tmp.join("csv/games2.csv").exists());

        let conn = Connection::open(tmp.join("sqlite.db")).unwrap();

        for table in ["games", "games2"] {
            let mut stmt = conn.prepare(&format!("select * from {}", table)).unwrap();
            let mut rows = stmt.query([]).unwrap();

            let mut output: Vec<(u64, String)> = vec![];
            while let Some(row) = rows.next().unwrap() {
                output.push((row.get(0).unwrap(), row.get(1).unwrap()));
            }
            insta::assert_yaml_snapshot!(output)
        }
    }

    #[test]
    fn test_csvs_to_sqlite() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        //let tmp = PathBuf::from("/tmp");

        let datapackage = csvs_to_sqlite_with_options(
            tmp.join("sqlite.db").to_string_lossy().into(),
            vec![
                "fixtures/add_resource/csv/games.csv".into(),
                "fixtures/add_resource/csv/games2.csv".into(),
            ],
            Options::builder().stats(true).build(),
        )
        .unwrap();

        assert!(tmp.join("sqlite.db").exists());
        let conn = Connection::open(tmp.join("sqlite.db")).unwrap();

        for table in ["games", "games2"] {
            let mut stmt = conn.prepare(&format!("select * from {}", table)).unwrap();
            let mut rows = stmt.query([]).unwrap();

            let mut output: Vec<(u64, String)> = vec![];
            while let Some(row) = rows.next().unwrap() {
                output.push((row.get(0).unwrap(), row.get(1).unwrap()));
            }
            insta::assert_yaml_snapshot!(output)
        }
        insta::assert_yaml_snapshot!(datapackage)
    }

    #[test]
    fn test_csvs_all_types_to_sqlite() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        csvs_to_sqlite_with_options(
            tmp.join("sqlite.db").to_string_lossy().into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
            Options::builder().stats(true).build(),
        )
        .unwrap();

        assert!(tmp.join("sqlite.db").exists());
    }

    #[test]
    fn test_csvs_to_sqlite_large() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        let options = Options::builder()
            .stats(true)
            .threads(8)
            .delimiter(Some(b','));

        csvs_to_sqlite_with_options(
            tmp.join("sqlite.db").to_string_lossy().into(),
            vec![
                "fixtures/large/csv/data.csv".into(),
                "fixtures/large/csv/daily_16.csv".into(),
                "fixtures/large/csv/data_weather.csv".into(),
            ],
            options.build(),
        )
        .unwrap();

        assert!(tmp.join("sqlite.db").exists());
    }

    #[test]
    fn test_ods_from_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        //let tmp = PathBuf::from("/tmp");

        csvs_to_ods(
            tmp.join("output.ods").to_string_lossy().into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
        )
        .unwrap();
    }


    #[test]
    fn test_xlsx_from_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        // let tmp = PathBuf::from("/tmp");

        csvs_to_xlsx(
            tmp.join("output.xlsx").to_string_lossy().into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn test_large_ods_from_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        // let tmp = PathBuf::from("/tmp");

        csvs_to_ods(
            tmp.join("output.ods").to_string_lossy().into(),
            vec![
                "fixtures/large/csv/data.csv".into(),
                "fixtures/large/csv/daily_16.csv".into(),
                "fixtures/large/csv/data_weather.csv".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn test_large_xlsx_from_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        csvs_to_xlsx(
            tmp.join("output.xlsx").to_string_lossy().into(),
            vec![
                "fixtures/large/csv/data.csv".into(),
                "fixtures/large/csv/daily_16.csv".into(),
                "fixtures/large/csv/data_weather.csv".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn test_ods() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        let options = Options::builder().delete_input_csv(true).build();

        std::fs::copy(
            "fixtures/add_resource/datapackage.json",
            tmp.join("datapackage.json"),
        )
        .unwrap();
        std::fs::create_dir_all(tmp.join("csv")).unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games.csv",
            tmp.join("csv/games.csv"),
        )
        .unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games2.csv",
            tmp.join("csv/games2.csv"),
        )
        .unwrap();

        datapackage_to_ods_with_options(
            tmp.join("output.ods").to_string_lossy().into(),
            tmp.to_string_lossy().into(),
            options,
        )
        .unwrap();
    }


    #[test]
    fn test_xlsx() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        let options = Options::builder().delete_input_csv(true).build();

        std::fs::copy(
            "fixtures/add_resource/datapackage.json",
            tmp.join("datapackage.json"),
        )
        .unwrap();
        std::fs::create_dir_all(tmp.join("csv")).unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games.csv",
            tmp.join("csv/games.csv"),
        )
        .unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games2.csv",
            tmp.join("csv/games2.csv"),
        )
        .unwrap();

        datapackage_to_xlsx_with_options(
            tmp.join("output.xlsx").to_string_lossy().into(),
            tmp.to_string_lossy().into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_large_ods() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        let options = Options::builder().build();

        datapackage_to_ods_with_options(
            tmp.join("output.ods").to_string_lossy().into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_large_xlsx() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        let options = Options::builder().build();

        datapackage_to_xlsx_with_options(
            tmp.join("output.xlsx").to_string_lossy().into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_multiple() {
        insta::assert_yaml_snapshot!(merge_datapackage_jsons(vec![
            "fixtures/base_datapackage/datapackage.json".into(),
            "fixtures/base_datapackage/datapackage.json".into(),
            "fixtures/add_different_resource/datapackage.json".into(),
            "fixtures/add_resource/datapackage.json".into(),
            "fixtures/add_field/datapackage.json".into(),
            "fixtures/conflict_types/datapackage.json".into()
        ])
        .unwrap());
    }

    #[test]
    fn test_csvs_db() {
        let options = Options::builder().drop(true).schema("test".into()).build();

        csvs_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_csvs_db_no_conn() {
        let options = Options::builder().drop(true).dump_file("/tmp/postgres_dump.sql".into()).build();

        csvs_to_postgres_with_options(
            "".into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
            options,
        )
        .unwrap();

        let file = File::open("/tmp/postgres_dump.sql").unwrap();
        let lines: Vec<String> = std::io::BufReader::new(file)
            .lines()
            .map(|x| x.unwrap())
            .collect();
        insta::assert_yaml_snapshot!(lines);
    }

    #[test]
    fn test_csvs_db_large() {
        let options = Options::builder().drop(true).schema("test".into()).build();

        csvs_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            vec![
                "fixtures/large/csv/data.csv".into(),
                "fixtures/large/csv/daily_16.csv".into(),
                "fixtures/large/csv/data_weather.csv".into(),
            ],
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_db_large() {
        let options = Options::builder()
            .drop(true)
            .schema("large_test".into())
            .build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_from_env() {
        let options = Options::builder()
            .drop(true)
            .schema("test_env".into())
            .build();

        std::env::set_var("POSTGRES_URL", "postgresql://test@localhost/test");

        datapackage_to_postgres_with_options(
            " env= POSTGRES_URL ".into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();

        let options = Options::builder()
            .drop(true)
            .schema("test_env".into())
            .build();

        std::env::set_var("DATABASE_URL", "postgresql://test@localhost/test");

        datapackage_to_postgres_with_options(" env  ".into(), "fixtures/large".into(), options)
            .unwrap();
    }

    // #[test]
    // fn test_postgres_types() {
    //     let options = Options::builder()
    //         .drop(true)
    //         .schema("types".into())
    //         .build();

    //     csvs_to_postgres_with_options("postgresql://test@localhost/test".into(), vec!["src/fixtures/date.csv".into()], options).unwrap();
    // }


    #[test]
    fn test_drop_postgres() {
        let options = Options::builder()
            .drop(true)
            .schema("test_drop2".into())
            .build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_truncate_postgres() {
        let options = Options::builder()
            .truncate(true)
            .schema("test_truncate".into())
            .build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/large".into(),
            options.clone(),
        )
        .unwrap();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_evolve_postgres() {
        let db_url = "postgresql://test@localhost/test";

        let mut client = Client::connect(db_url, NoTls).unwrap();

        let options = Options::builder()
            .drop(true)
            .schema("evolve".into())
            .build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let result = client
            .query_one("select * from evolve.evolve limit 1", &[])
            .unwrap();
        let name_type: Vec<String> = result
            .columns()
            .iter()
            .map(|a| format!("{}-{}", a.name(), a.type_()))
            .collect();
        insta::assert_yaml_snapshot!(name_type);

        let options = Options::builder()
            .evolve(true)
            .schema("evolve".into())
            .build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let result = client
            .query_one("select * from evolve.evolve limit 1", &[])
            .unwrap();
        let name_type: Vec<String> = result
            .columns()
            .iter()
            .map(|a| format!("{}-{}", a.name(), a.type_()))
            .collect();
        insta::assert_yaml_snapshot!(name_type);

        let options = Options::builder()
            .evolve(true)
            .schema("evolve".into())
            .build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/first".into(),
            options,
        )
        .unwrap();

        let result = client
            .query_one("select * from evolve.evolve limit 1", &[])
            .unwrap();
        let name_type: Vec<String> = result
            .columns()
            .iter()
            .map(|a| format!("{}-{}", a.name(), a.type_()))
            .collect();
        insta::assert_yaml_snapshot!(name_type);

        let options = Options::builder()
            .evolve(true)
            .schema("evolve".into())
            .build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/second".into(),
            options,
        )
        .unwrap();

        let result = client
            .query_one("select * from evolve.evolve limit 1", &[])
            .unwrap();
        let name_type: Vec<String> = result
            .columns()
            .iter()
            .map(|a| format!("{}-{}", a.name(), a.type_()))
            .collect();
        insta::assert_yaml_snapshot!(name_type);
    }

    #[test]
    fn test_drop_sqlite() {
        let options = Options::builder().drop(true).build();
        datapackage_to_sqlite_with_options(
            "/tmp/sqlite.db".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let options = Options::builder().drop(true).build();
        datapackage_to_sqlite_with_options(
            "/tmp/sqlite.db".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_truncate_sqlite() {
        let options = Options::builder().truncate(true).build();
        datapackage_to_sqlite_with_options(
            "/tmp/sqlite.db".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let options = Options::builder().truncate(true).build();
        datapackage_to_sqlite_with_options(
            "/tmp/sqlite.db".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_csvs_sqlite_no_conn() {
        let options = Options::builder().drop(true).dump_file("/tmp/sqlite_dump.sql".into()).build();

        csvs_to_sqlite_with_options(
            "".into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
            options,
        )
        .unwrap();
        
        let file = File::open("/tmp/sqlite_dump.sql").unwrap();
        let lines: Vec<String> = std::io::BufReader::new(file)
            .lines()
            .map(|x| x.unwrap())
            .collect();
        insta::assert_yaml_snapshot!(lines);
    }

    #[test]
    fn test_evolve_sqlite() {
        let options = Options::builder().drop(true).build();

        datapackage_to_sqlite_with_options(
            "/tmp/evolve.db".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let output = check_evolve();
        insta::assert_yaml_snapshot!(output);

        let options = Options::builder().evolve(true).build();

        datapackage_to_sqlite_with_options(
            "/tmp/evolve.db".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let output = check_evolve();
        insta::assert_yaml_snapshot!(output);

        let options = Options::builder().evolve(true).build();

        datapackage_to_sqlite_with_options(
            "/tmp/evolve.db".into(),
            "fixtures/evolve/first".into(),
            options,
        )
        .unwrap();

        let output = check_evolve();
        insta::assert_yaml_snapshot!(output);

        let options = Options::builder().evolve(true).build();

        datapackage_to_sqlite_with_options(
            "/tmp/evolve.db".into(),
            "fixtures/evolve/second".into(),
            options,
        )
        .unwrap();

        let output = check_evolve();
        insta::assert_yaml_snapshot!(output);
    }

    fn check_evolve() -> Vec<Vec<String>> {
        let conn = Connection::open("/tmp/evolve.db").unwrap();
        let mut stmt = conn.prepare("select * from evolve").unwrap();
        let count = stmt.column_count();
        let mut rows = stmt.query([]).unwrap();
        let mut output = vec![];
        while let Some(row) = rows.next().unwrap() {
            let mut row_data: Vec<String> = vec![];
            for i in 0..count {
                let value = row.get_ref_unwrap(i);
                match value {
                    ValueRef::Text(text) => {
                        row_data.push(std::str::from_utf8(text).unwrap().to_owned())
                    }
                    ValueRef::Integer(num) => row_data.push(num.to_string()),
                    other => row_data.push(format!("{:?}", other)),
                }
            }
            output.push(row_data);
        }
        output
    }

        #[test]
    fn test_parquet() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        let options = Options::builder().delete_input_csv(true).build();

        std::fs::copy(
            "fixtures/add_resource/datapackage.json",
            tmp.join("datapackage.json"),
        )
        .unwrap();
        std::fs::create_dir_all(tmp.join("csv")).unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games.csv",
            tmp.join("csv/games.csv"),
        )
        .unwrap();
        std::fs::copy(
            "fixtures/add_resource/csv/games2.csv",
            tmp.join("csv/games2.csv"),
        )
        .unwrap();

        datapackage_to_parquet_with_options(
            tmp.join("parquet"),
            tmp.to_string_lossy().into(),
            options,
        )
        .unwrap();

        assert!(tmp.join("parquet/games.parquet").exists());
        assert!(tmp.join("parquet/games2.parquet").exists());
        assert!(!tmp.join("csv/games.csv").exists());
        assert!(!tmp.join("csv/games2.csv").exists());

        let games1 = File::open(tmp.join("parquet/games.parquet")).unwrap();
        let games2 = File::open(tmp.join("parquet/games2.parquet")).unwrap();

        for file in [games1, games2] {
            let reader = SerializedFileReader::new(file).unwrap();

            let mut data = vec![];
            for row in reader {
                for (_idx, (name, field)) in row.unwrap().get_column_iter().enumerate() {
                    let field = match field {
                        parquet::record::Field::Str(string) => string.to_owned(),
                        other => other.to_string(),
                    };
                    data.push((name.to_owned(), field));
                }
            }
            insta::assert_yaml_snapshot!(data)
        }
    }

    #[test]
    fn test_parquet_from_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        csvs_to_parquet(
            tmp.join("parquet").to_string_lossy().into(),
            vec![
                "fixtures/add_resource/csv/games.csv".into(),
                "fixtures/add_resource/csv/games2.csv".into(),
            ],
        )
        .unwrap();

        assert!(tmp.join("parquet/games.parquet").exists());
        assert!(tmp.join("parquet/games2.parquet").exists());

        let games1 = File::open(tmp.join("parquet/games.parquet")).unwrap();
        let games2 = File::open(tmp.join("parquet/games2.parquet")).unwrap();

        for file in [games1, games2] {
            let reader = SerializedFileReader::new(file).unwrap();

            let mut data = vec![];
            for row in reader {
                for (_idx, (name, field)) in row.unwrap().get_column_iter().enumerate() {
                    let field = match field {
                        parquet::record::Field::Str(string) => string.to_owned(),
                        other => other.to_string(),
                    };
                    data.push((name.to_owned(), field));
                }
            }
            insta::assert_yaml_snapshot!(data)
        }
    }

    #[test]
    fn test_parquet_all_types_from_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        //let tmp = PathBuf::from("/tmp");

        csvs_to_parquet(
            tmp.join("parquet").to_string_lossy().into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn test_parquet_all_types_from_csvs_as_strings() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        //let tmp = PathBuf::from("/tmp");

        let options = Options::builder().all_strings(false).build();

        csvs_to_parquet_with_options(
            tmp.join("parquet").to_string_lossy().into(),
            vec![
                "src/fixtures/all_types.csv".into(),
                "src/fixtures/all_types_semi_colon.csv".into(),
            ],
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_parquet_dates_from_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();
        //let tmp = PathBuf::from("/tmp");

        let _res = csvs_to_parquet(
            tmp.join("parquet").to_string_lossy().into(),
            vec!["fixtures/parquet_date.csv".into()],
        )
        .unwrap();
    }
}

