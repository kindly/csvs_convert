mod zip_dir;

use csv::ReaderBuilder;
use csv::Writer;
use minijinja::Environment;
use rusqlite::Connection;
use serde_json::Value;
use snafu::prelude::*;
use snafu::{ensure, Snafu};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use tempfile::TempDir;
use typed_builder::TypedBuilder;
use xlsxwriter::Workbook;
use std::io::Write;
use std::fmt::Write as fmt_write;
use postgres::{Client, NoTls};

use arrow::csv::Reader;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use parquet::{
    arrow::ArrowWriter, basic::Compression, errors::ParquetError,
    file::properties::WriterProperties,
};

#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{}", message))]
    DatapackageMergeError { message: String },

    #[snafu(display("{}", message))]
    DatapackageXLSXError { message: String },

    #[snafu(display("Error reading file {}: {}", filename, source))]
    IoError {
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
    RusqliteError { source: rusqlite::Error, message: String},

    #[snafu(display("{}", source))]
    JinjaError { source: minijinja::Error },

    #[snafu(display("{}", source))]
    ParquetError { source: ParquetError },

    #[snafu(display("{}", source))]
    ArrowError { source: ArrowError },

    #[snafu(display("Postgres Error: {}", source))]
    PostgresError { source: postgres::Error },

    #[snafu(display("Error with writing XLSX file"))]
    XLSXError { source: xlsxwriter::XlsxError },

    #[snafu(display("Environment variable {} does not exist.", envvar))]
    EnvVarError { source: std::env::VarError, envvar: String },

    #[snafu(display("Delimeter not valid utf-8"))]
    DelimeiterError { source: std::str::Utf8Error },
}

#[derive(Default, Debug, TypedBuilder)]
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
    #[builder(default=b',')]
    pub delimiter: u8,
    #[builder(default=b'"')]
    pub quote: u8,
    #[builder(default=true)]
    pub double_quote: bool,
    #[builder(default=None)]
    pub escape: Option<u8>,
    #[builder(default=None)]
    pub comment: Option<u8>,
}

lazy_static::lazy_static! {
    #[allow(clippy::invalid_regex)]
    pub static ref INVALID_REGEX: regex::Regex = regex::RegexBuilder::new(r"[\000-\010]|[\013-\014]|[\016-\037]")
        .octal(true)
        .build()
        .unwrap();
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
    for field in fields_option.unwrap().drain(..) {
        let name_option = field["name"].as_str();
        ensure!(
            name_option.is_some(),
            DatapackageMergeSnafu {
                message: "Each field needs a name"
            }
        );
        new_fields.insert(name_option.unwrap().to_owned(), field);
    }

    resource["schema"]
        .as_object_mut()
        .unwrap()
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
    for resource in resources_option.unwrap().drain(..) {
        let path;
        {
            let path_str = resource["path"].as_str();
            ensure!(
                path_str.is_some(),
                DatapackageMergeSnafu {
                    message: "datapackage resource needs a name or path"
                }
            );
            path = path_str.unwrap().to_owned();
        }

        let new_resource = make_mergeable_resource(resource)?;
        new_resources.insert(path, new_resource);
    }

    value
        .as_object_mut()
        .unwrap()
        .insert("resources".into(), new_resources.into());

    Ok(value)
}

fn make_datapackage_from_mergeable(mut value: Value) -> Result<Value, Error> {
    let mut resources = value["resources"].take();

    let resources_option = resources.as_object_mut();

    let mut new_resources = vec![];
    for resource in resources_option.unwrap().values_mut() {
        let new_resource = make_resource_from_mergable(resource.clone())?;
        new_resources.push(new_resource);
    }

    value
        .as_object_mut()
        .unwrap()
        .insert("resources".into(), new_resources.into());

    Ok(value)
}

fn make_resource_from_mergable(mut resource: Value) -> Result<Value, Error> {
    let mut fields = resource["schema"]["fields"].take();
    let fields_option = fields.as_object_mut();

    let mut new_fields = vec![];
    for field in fields_option.unwrap().values_mut() {
        new_fields.push(field.clone());
    }

    resource["schema"]
        .as_object_mut()
        .unwrap()
        .insert("fields".to_string(), new_fields.into());

    Ok(resource)
}

fn datapackage_json_to_value(filename: &str) -> Result<Value, Error> {
    if filename.ends_with(".json") {
        let file = File::open(&filename).context(IoSnafu { filename })?;
        let json: Value =
            serde_json::from_reader(BufReader::new(file)).context(JSONSnafu { filename })?;
        Ok(json)
    } else if filename.ends_with(".zip") {
        let file = File::open(&filename).context(IoSnafu { filename })?;
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

    let merger_resources = merger_resources_value.as_object().unwrap();
    let base_resources = base["resources"].as_object_mut().unwrap();

    for (resource, resource_value) in merger_resources {
        if !base_resources.contains_key(resource) {
            base_resources.insert(resource.clone(), resource_value.clone());
        } else {
            for (field, field_value) in resource_value["schema"]["fields"].as_object().unwrap() {
                ensure!(
                    field_value.is_object(),
                    DatapackageMergeSnafu {
                        message: "Each field needs to be an object"
                    }
                );

                let base_fields = base_resources[resource]["schema"]["fields"]
                    .as_object_mut()
                    .unwrap();

                if !base_fields.contains_key(field) {
                    base_fields.insert(field.clone(), field_value.clone());
                } else {
                    ensure!(
                        base_fields[field].is_object(),
                        DatapackageMergeSnafu {
                            message: "Each field needs to be an object"
                        }
                    );
                    let base_fieldinfo = base_fields[field].as_object_mut().unwrap();

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
                Some(index) => output_row.push(row.get(*index).unwrap()),
                None => output_row.push(""),
            }
        }
        csv_writer
            .write_record(output_row)
            .context(CSVRowSnafu {})?;
    }
    Ok(csv_writer)
}

enum Readers {
    File((PathBuf, File)),
    Zip(zip::ZipArchive<File>),
}

fn get_reader(file: &str, resource_path: &str) -> Result<Readers, Error> {
    if file.ends_with(".json") {
        let mut file_pathbuf = PathBuf::from(file);
        file_pathbuf.pop();
        file_pathbuf.push(&resource_path);
        Ok(Readers::File((
            file_pathbuf.clone(),
            File::open(&file_pathbuf).context(IoSnafu {
                filename: file_pathbuf.to_string_lossy(),
            })?,
        )))
    } else if file.ends_with(".zip") {
        let zip_file = File::open(&file).context(IoSnafu {
            filename: file,
        })?;
        let zip = zip::ZipArchive::new(zip_file).context(ZipSnafu {
            filename: file,
        })?;
        Ok(Readers::Zip(zip))
    } else if PathBuf::from(&file).is_dir() {
        let file_pathbuf = PathBuf::from(file);
        let file_pathbuf = file_pathbuf.join(&resource_path);
        Ok(Readers::File((
            file_pathbuf.clone(),
            File::open(&file_pathbuf).context(IoSnafu {
                filename: file_pathbuf.to_string_lossy(),
            })?,
        )))
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

    for resource in merged_datapackage_json["resources"].as_array_mut().unwrap() {
        let mut field_order_map = serde_json::Map::new();
        let mut fields: Vec<String> = Vec::new();
        for (index, field) in resource["schema"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            let name = field["name"].as_str().unwrap();
            field_order_map.insert(name.into(), index.into());
            fields.push(name.to_owned());
        }

        let resource_path = resource["path"].as_str().unwrap().to_owned();

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
            .unwrap()
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

            let csv_readers = get_reader(file, &resource_path)?;

            match csv_readers {
                Readers::Zip(mut zip) => {
                    let zipped_file = zip
                        .by_name(&resource_path)
                        .context(ZipSnafu { filename: file })?;
                    let csv_reader = get_csv_reader_builder(&options).from_reader(zipped_file);
                    csv_output =
                        write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields)?;
                }
                Readers::File(file_reader) => {
                    let (filename, file_reader) = file_reader;
                    let csv_reader = get_csv_reader_builder(&options).from_reader(file_reader);
                    csv_output =
                        write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields)?;
                    if options.delete_input_csv {
                        std::fs::remove_file(&filename).context(IoSnafu {
                            filename: filename.to_string_lossy(),
                        })?;
                    }
                }
            }

            csv_outputs.insert(resource_path, csv_output);
        }
    }

    for (name, csv_file) in csv_outputs.iter_mut() {
        csv_file.flush().context(IoSnafu { filename: name })?;
    }

    if tmpdir_option.is_some() {
        zip_dir::zip_dir(&output_path, &original_path).context(ZipSnafu {
            filename: original_path.to_string_lossy(),
        })?;
    }

    Ok(())
}

fn get_csv_reader_builder(options: &Options) -> csv::ReaderBuilder {
    let mut reader_builder = ReaderBuilder::new();

    reader_builder 
        .delimiter(options.delimiter)
        .quote(options.quote)
        .double_quote(options.double_quote)
        .escape(options.escape)
        .comment(options.comment);

    reader_builder
}

fn to_db_type(value: &str) -> String {
    match value {
        "string" => "TEXT".to_string(),
        "date" => "TIMESTAMP".to_string(),
        "number" => "NUMERIC".to_string(),
        "boolean" => "BOOL".to_string(),
        _ => "TEXT".to_string(),
    }
}

fn to_sqlite_type(_state: &minijinja::State, value: String) -> Result<String, minijinja::Error> {
    Ok(to_db_type(&value))
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
           {% if not loop.first %}, {% endif %}[{{field.name}}] {{field.type | sqlite_type}} #nl
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
                , FOREIGN KEY ([{{foreignKey.fields}}]) REFERENCES [{{foreignKey.reference.resource}}]([{{foreignKey.reference.fields}}]) #nl
              {% endif %}
              {% if foreignKey.fields is sequence %}
                , FOREIGN KEY ([{{foreignKey.fields | join("],[")}}]) 
                  REFERENCES [{{foreignKey.reference.resource}}]([{{foreignKey.reference.fields | join("],[")}}]) #nl
              {% endif %}
           {% endfor %}
        {% endif %}
    ); #nl

    {% if schema.foreignKeys is sequence %}
        {% for foreignKey in schema.foreignKeys %}
            {% if foreignKey.fields is string %}
              CREATE INDEX [idx_{{name}}_{{foreignKey.fields}}] ON [{{name}}] ([{{foreignKey.fields}}]); #nl
            {% endif %}
            {% if foreignKey.fields is sequence %}
              CREATE INDEX [idx_{{name}}_{{foreignKey.fields | join("_")}}] ON [{{name}}] ([{{foreignKey.fields | join("],[")}}]); #nl
            {% endif %}
        {% endfor %}
    {% endif %}

    "#;
    let sqlite_table = sqlite_table.replace("  ", "");
    let sqlite_table = sqlite_table.replace('\n', "");
    let sqlite_table = sqlite_table.replace("#nl", "\n");

    let mut env = Environment::new();
    env.add_filter("sqlite_type", to_sqlite_type);
    env.add_template("sqlite_resource", &sqlite_table).unwrap();
    let tmpl = env.get_template("sqlite_resource").unwrap();
    tmpl.render(value).context(JinjaSnafu {})
}

fn render_postgres_table(value: Value) -> Result<String, Error> {
    let postgres_table = r#"
    CREATE TABLE IF NOT EXISTS "{{title|default(name)}}" (
        {% for field in schema.fields %}
           {% if not loop.first %}, {% endif %}"{{field.name|clean_field}}" {{field.type | postgres_type}} #nl
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
              CREATE INDEX "idx_{{title|default(name)}}_{{foreignKey.fields}}" ON "{{title|default(name)}}" ("{{foreignKey.fields}}"); #nl
            {% endif %}
            {% if foreignKey.fields is sequence %}
              CREATE INDEX "idx_{{title|default(name)}}_{{foreignKey.fields | join("_")}}" ON "{{title|default(name)}}" ("{{foreignKey.fields | join('","')}}"); #nl
            {% endif %}
        {% endfor %}
    {% endif %}

    "#;
    let postgres_table = postgres_table.replace("  ", "");
    let postgres_table = postgres_table.replace('\n', "");
    let postgres_table = postgres_table.replace("#nl", "\n");

    let mut env = Environment::new();
    env.add_filter("postgres_type", to_sqlite_type);
    env.add_filter("clean_field", clean_field);
    env.add_template("postgres_resource", &postgres_table).unwrap();
    let tmpl = env.get_template("postgres_resource").unwrap();
    tmpl.render(value).context(JinjaSnafu {})
}


fn insert_sql_data(
    csv_reader: csv::Reader<impl std::io::Read>,
    mut conn: rusqlite::Connection,
    resource: Value,
) -> Result<rusqlite::Connection, Error> {
    let tx = conn.transaction().context(RusqliteSnafu {message: "Error making transaction: "})?;

    let table = resource["name"].as_str().unwrap();

    let mut fields_len = 0;
    let mut fields = vec![];

    if let Some(fields_vec) = resource["schema"]["fields"].as_array() {
        fields_len = fields_vec.len();
        for field_value in fields_vec {
            if let Some(field) = field_value["name"].as_str(){
                fields.push(format!("[{field}]"))
            }
        }
    };

    let fields = fields.join(", ");

    let mut question_marks = "?,".repeat(fields_len);

    question_marks.pop();

    {
        let mut statement = tx
            .prepare_cached(&format!("INSERT INTO [{table}]({fields}) VALUES ({question_marks})"))
            .context(RusqliteSnafu {message: "Error preparing sqlite statment: "})?;

        for row in csv_reader.into_deserialize() {
            let this_row: Vec<String> = row.context(CSVSnafu { filename: table })?;

            statement
                .execute(rusqlite::params_from_iter(this_row.iter()))
                .context(RusqliteSnafu {message: "Error inserting data to sqlite: "})?;
        }
    }
    tx.commit().context(RusqliteSnafu {message: "Error commiting sqlite: "})?;
    Ok(conn)
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
    let (table_to_schema, ordered_tables) = get_table_info(&datapackage)?;

    let mut conn = Connection::open(db_path).context(RusqliteSnafu {message: "Error opening connection: "})?;

    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = 0;
         PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA temp_store = MEMORY;",
    )
    .context(RusqliteSnafu {message: "Error executing pragmas: "})?;


    for table in ordered_tables {
        let resource = table_to_schema.get(&table).unwrap();

        let mut existing_columns: HashMap<String, String> = HashMap::new();

        {
            let mut fields_query = conn.prepare("select name, type from pragma_table_info(?)").context(RusqliteSnafu {message: "Error peparing sql"})?;

            let mut rows = fields_query.query(
                [&table],
            ).context(RusqliteSnafu {message: "Error peparing sql"})?;

            while let Some(row) = rows.next().context(RusqliteSnafu {message: "Error fetching rows"})? {
                existing_columns.insert(
                    row.get(0).context(RusqliteSnafu {message: "Error fetching rows"})?,
                    row.get(1).context(RusqliteSnafu {message: "Error fetching rows"})?,
                );
            }
        }

        let mut create = false;

        if existing_columns.len() == 0 {
            create = true
        } else if options.drop {
            conn.execute(&format!("drop table [{table}];"), [])
                .context(RusqliteSnafu {message: "Error making sqlite tables: "})?;
            create = true
        }

        ensure!(
            resource["path"].is_string(),
            DatapackageMergeSnafu {
                message: "Datapackages resources need a `path`"
            }
        );

        if create {
            let resource_sqlite = render_sqlite_table(resource.clone())?;

            conn.execute(&resource_sqlite, [])
                .context(RusqliteSnafu {message: "Error making sqlite tables: "})?;
        } else if options.evolve {
            let (add_columns, _alter_columns) = get_column_changes(resource, existing_columns);
            for (name, type_) in add_columns {
                conn.execute(&format!("ALTER TABLE {table} ADD [{name}] {type_}"), []).context(RusqliteSnafu {message: "Error altering sqlite tables: "})?;
            }
        }

        let resource_path = resource["path"].as_str().unwrap();

        let csv_readers = get_reader(&datapackage, resource_path)?;

        match csv_readers {
            Readers::Zip(mut zip) => {
                let zipped_file = zip.by_name(resource_path).context(ZipSnafu {
                    filename: &datapackage,
                })?;
                let csv_reader = get_csv_reader_builder(&options).from_reader(zipped_file);
                conn = insert_sql_data(csv_reader, conn, resource.clone())?
            }
            Readers::File(csv_file) => {
                let (filename, file) = csv_file;
                let csv_reader = get_csv_reader_builder(&options).from_reader(file);
                conn = insert_sql_data(csv_reader, conn, resource.clone())?;
                if options.delete_input_csv {
                    std::fs::remove_file(&filename).context(IoSnafu {
                        filename: filename.to_string_lossy(),
                    })?;
                }
            }
        }
    }

    Ok(())
}

fn get_table_info(datapackage: &str) -> Result<(HashMap<String, Value>, Vec<String>), Error> {
    let mut datapackage_value = datapackage_json_to_value(datapackage)?;
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
    let mut relationhip_graph = petgraph::graphmap::DiGraphMap::new();
    for (x, y) in table_links.iter() {
        relationhip_graph.add_edge(y, x, 1);
    }
    let ordered_tables = petgraph::algo::kosaraju_scc(&relationhip_graph);
    let tables: Vec<String> = ordered_tables.into_iter().flatten().map(|x| x.to_owned()).collect();
    Ok((table_to_schema, tables))
}

fn create_parquet(
    file: impl std::io::Read,
    resource: Value,
    mut output_path: PathBuf,
    options: &Options
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

        let field = match field_type {
            "number" => Field::new(name, DataType::Float64, true),
            "boolean" => Field::new(name, DataType::Boolean, true),
            _ => Field::new(name, DataType::Utf8, true),
        };
        arrow_fields.push(field);
    }
    

    let arrow_csv_reader = Reader::new(
        file,
        std::sync::Arc::new(Schema::new(arrow_fields)),
        true,
        Some(options.delimiter),
        1024,
        None,
        None,
        None,
    );

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

pub fn datapackage_to_parquet(output_path: PathBuf, datapackage: String) -> Result<(), Error> {
    let options = Options::builder().build();
    datapackage_to_parquet_with_options(output_path, datapackage, options)
}

pub fn datapackage_to_parquet_with_options(
    output_path: PathBuf,
    datapackage: String,
    options: Options,
) -> Result<(), Error> {
    std::fs::create_dir_all(&output_path).context(IoSnafu {
        filename: output_path.to_string_lossy(),
    })?;

    let mut datapackage_value = datapackage_json_to_value(&datapackage)?;

    let resources_option = datapackage_value["resources"].as_array_mut();
    ensure!(
        resources_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `resources` key as an array"
        }
    );

    for resource in resources_option.unwrap() {
        let resource_path = resource["path"].as_str().unwrap();

        let csv_readers = get_reader(&datapackage, resource_path)?;

        match csv_readers {
            Readers::Zip(mut zip) => {
                let zipped_file = zip.by_name(resource_path).context(ZipSnafu {
                    filename: &datapackage,
                })?;
                create_parquet(zipped_file, resource.clone(), output_path.clone(), &options)?
            }
            Readers::File(csv_reader) => {
                let (filename, file) = csv_reader;
                create_parquet(file, resource.clone(), output_path.clone(), &options)?;
                if options.delete_input_csv {
                    std::fs::remove_file(&filename).context(IoSnafu {
                        filename: filename.to_string_lossy(),
                    })?;
                }
            }
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
    let bold = workbook.add_format().set_bold();

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

    let mut worksheet = workbook
        .add_worksheet(Some(&new_title))
        .context(XLSXSnafu {})?;

    for (row_num, row) in csv_reader.into_records().enumerate() {
        let this_row = row.context(CSVSnafu { filename: &title })?;

        let mut format = None;

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
            format = Some(&bold);
        }

        for (col_index, value) in this_row.iter().enumerate() {
            let mut cell = value.to_string();

            if field_types[col_index] == "number" {
                if let Ok(number) = value.parse::<f64>() {
                    worksheet
                        .write_number(
                            row_num.try_into().unwrap(),
                            col_index.try_into().unwrap(),
                            number,
                            format,
                        )
                        .context(XLSXSnafu {})?;
                    continue;
                }
            }

            if INVALID_REGEX.is_match(&cell) {
                cell = INVALID_REGEX.replace_all(&cell, "").to_string();
            }

            worksheet
                .write_string(
                    row_num.try_into().unwrap(),
                    col_index.try_into().unwrap(),
                    &cell,
                    format,
                )
                .context(XLSXSnafu {})?;
        }
    }
    Ok(())
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
    let mut datapackage_value = datapackage_json_to_value(&datapackage)?;

    let resources_option = datapackage_value["resources"].as_array_mut();
    ensure!(
        resources_option.is_some(),
        DatapackageMergeSnafu {
            message: "Datapackages need a `resources` key as an array"
        }
    );

    let mut pathbuf = PathBuf::from(&xlsx_path);
    pathbuf.pop();

    let mut workbook =
        Workbook::new_with_options(&xlsx_path, true, Some(&pathbuf.to_string_lossy()), false);

    for resource in resources_option.unwrap() {
        let resource_path = resource["path"].as_str().unwrap();

        let csv_readers = get_reader(&datapackage, resource_path)?;

        match csv_readers {
            Readers::Zip(mut zip) => {
                let zipped_file = zip.by_name(resource_path).context(ZipSnafu {
                    filename: &datapackage,
                })?;
                let csv_reader = get_csv_reader_builder(&options).has_headers(false).from_reader(zipped_file);

                create_sheet(csv_reader, resource.clone(), &mut workbook, &options)?;
            }
            Readers::File(csv_file) => {
                let (filename, file) = csv_file;
                let csv_reader = get_csv_reader_builder(&options).has_headers(false).from_reader(file);
                if options.delete_input_csv {
                    std::fs::remove_file(&filename).context(IoSnafu {
                        filename: filename.to_string_lossy(),
                    })?;
                }
                create_sheet(csv_reader, resource.clone(), &mut workbook, &options)?;
            }
        }
    }

    Ok(())
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
    let (table_to_schema, ordered_tables) = get_table_info(&datapackage)?;

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
        if !env.is_empty()  {
            conf = std::env::var(env).context(EnvVarSnafu {envvar: env.to_owned()})?;
        }
    }

    let mut client = Client::connect(&conf, NoTls).context(PostgresSnafu {})?;

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
            resource_postgres = format!("
                CREATE SCHEMA IF NOT EXISTS \"{schema}\";
                set search_path = \"{schema}\";  
                {resource_postgres};
            ", schema=options.schema);
            schema_table = format!("\"{schema}\".\"{table}\"", schema=options.schema);
        }

        let result = client.query_one("SELECT to_regclass($1)::TEXT", &[&schema_table]).context(PostgresSnafu {})?;
        let exists: Option<String> = result.get(0);

        let mut create = exists.is_none();

        let mut drop = options.drop;

        let mut existing_columns= None;

        if !create && options.evolve {
            let result = client.query_opt(&format!("SELECT * FROM {schema_table} limit 1"), &[]).context(PostgresSnafu {})?;
            if result.is_none(){
                drop=true
            }
            if let Some(row) = result {
                let mut columns = HashMap::new();
                for column in row.columns() {
                    columns.insert(column.name().to_owned(), column.type_().to_string());
                }
                existing_columns = Some(columns)
            }
        }

        if drop && exists.is_some() {
            create = true;
            let mut drop_statement = String::new();
            if !options.schema.is_empty() {
                write!(drop_statement, "set search_path = \"{schema}\";", schema=options.schema).unwrap();
            }
            write!(drop_statement, "DROP TABLE IF EXISTS \"{table}\" CASCADE;").unwrap();
            client.batch_execute(&drop_statement).context(PostgresSnafu {})?
        }

        if create {
            client.batch_execute(&resource_postgres).context(PostgresSnafu {})?;
        }

        let mut columns = vec![];
        if let Some(fields) = resource["schema"]["fields"].as_array() {
            for field in fields {
                if let Some(name) = field["name"].as_str() {
                    columns.push(format!("\"{name}\""));
                }
            }
        }
        let all_columns = columns.join(", ");


        if let Some(existing_columns) = existing_columns {
            let (add_columns, alter_columns) = get_column_changes(resource, existing_columns);
            for (name, type_) in add_columns {
                client.batch_execute(&format!("ALTER TABLE {schema_table} ADD COLUMN \"{name}\" {type_}")).context(PostgresSnafu {})?;
            }

            for name in alter_columns {
                client.batch_execute(&format!("ALTER TABLE {schema_table} ALTER COLUMN \"{name}\" TYPE TEXT")).context(PostgresSnafu {})?;
            }
        }


        let csv_readers = get_reader(&datapackage, resource_path)?;
        let delimeter = std::str::from_utf8(&[options.delimiter]).context(DelimeiterSnafu {})?.to_owned();

        match csv_readers {
            Readers::Zip(mut zip) => {
                let mut zipped_file = zip.by_name(resource_path).context(ZipSnafu {
                    filename: &datapackage,
                })?;

                let mut writer = client.copy_in(&format!("copy {schema_table}({all_columns}) from STDIN WITH CSV HEADER DELIMITER '{delimeter}'")).context(PostgresSnafu {})?;
                std::io::copy(&mut zipped_file, &mut writer).context(IoSnafu {filename: resource_path})?;
            }
            Readers::File(csv_file) => {
                let (filename, mut file) = csv_file;
                let mut writer = client.copy_in(&format!("copy {schema_table}({all_columns}) from STDIN WITH CSV HEADER DELIMITER '{delimeter}'")).context(PostgresSnafu {})?;
                std::io::copy(&mut file, &mut writer).context(IoSnafu {filename: resource_path})?;
                file.flush().unwrap();
                writer.finish().unwrap();

                if options.delete_input_csv {
                    std::fs::remove_file(&filename).context(IoSnafu {
                        filename: filename.to_string_lossy(),
                    })?;
                }
            }
        }
    }

    Ok(())
}

fn get_column_changes(resource: &Value, existing_columns: HashMap<String, String>) -> (Vec<(String, String)>, Vec<String>){
    let mut add_columns = vec![];
    let mut alter_columns = vec![];
    if let Some(fields) = resource["schema"]["fields"].as_array() {
        for field in fields {
            if let Some(name) = field["name"].as_str() {
                if let Some(type_) = field["type"].as_str() {
                    let existing_column_type = existing_columns.get(name);
                    if let Some(existing_column_type) = existing_column_type {
                        if to_db_type(type_).to_lowercase() != existing_column_type.to_lowercase() {
                            alter_columns.push(name.to_owned());
                        }
                    } else {
                        add_columns.push((name.to_owned(), to_db_type(type_)))
                    }
                }
            }
        }
    }
    return (add_columns, alter_columns)
}


#[cfg(test)]
mod tests {
    use super::*;
    
    use parquet::file::reader::SerializedFileReader;
    use rusqlite::types::ValueRef;
    use std::io::BufRead;

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
                for (_idx, (name, field)) in row.get_column_iter().enumerate() {
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
    fn test_db_large() {
        let options = Options::builder().drop(true).schema("test".into()).build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_from_env() {
        let options = Options::builder().drop(true).schema("test_env".into()).build();

        std::env::set_var("POSTGRES_URL", "postgresql://test@localhost/test");

        datapackage_to_postgres_with_options(
            " env= POSTGRES_URL ".into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();

        let options = Options::builder().drop(true).schema("test_env".into()).build();

        std::env::set_var("DATABASE_URL", "postgresql://test@localhost/test");

        datapackage_to_postgres_with_options(
            " env  ".into(),
            "fixtures/large".into(),
            options,
        )
        .unwrap();
    }

    #[test]
    fn test_drop_postgres() {
        let options = Options::builder().drop(true).schema("test_drop2".into()).build();

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

        let options = Options::builder().drop(true).schema("evolve".into()).build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let result = client.query_one("select * from evolve.evolve limit 1", &[]).unwrap();
        let name_type: Vec<String> = result.columns().iter().map(|a| format!("{}-{}", a.name(), a.type_())).collect();
        insta::assert_yaml_snapshot!(name_type);

        let options = Options::builder().evolve(true).schema("evolve".into()).build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/base".into(),
            options,
        )
        .unwrap();

        let result = client.query_one("select * from evolve.evolve limit 1", &[]).unwrap();
        let name_type: Vec<String> = result.columns().iter().map(|a| format!("{}-{}", a.name(), a.type_())).collect();
        insta::assert_yaml_snapshot!(name_type);

        let options = Options::builder().evolve(true).schema("evolve".into()).build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/first".into(),
            options,
        )
        .unwrap();

        let result = client.query_one("select * from evolve.evolve limit 1", &[]).unwrap();
        let name_type: Vec<String> = result.columns().iter().map(|a| format!("{}-{}", a.name(), a.type_())).collect();
        insta::assert_yaml_snapshot!(name_type);

        let options = Options::builder().evolve(true).schema("evolve".into()).build();

        datapackage_to_postgres_with_options(
            "postgresql://test@localhost/test".into(),
            "fixtures/evolve/second".into(),
            options,
        )
        .unwrap();

        let result = client.query_one("select * from evolve.evolve limit 1", &[]).unwrap();
        let name_type: Vec<String> = result.columns().iter().map(|a| format!("{}-{}", a.name(), a.type_())).collect();
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
        let mut output= vec![];
        while let Some(row) = rows.next().unwrap() {
            let mut row_data: Vec<String> = vec![];
            for i in 0..count {
                let value = row.get_ref_unwrap(i);
                match value {
                    ValueRef::Text(text) => {row_data.push(std::str::from_utf8(text).unwrap().to_owned())} 
                    ValueRef::Integer(num) => {row_data.push(num.to_string())}
                    other => {row_data.push(format!("{:?}", other))} 
                }
            }
            output.push(row_data);
        }
        output
    }

}
