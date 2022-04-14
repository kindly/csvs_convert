mod zip_dir;

use csv::Writer;
use minijinja::Environment;
use rusqlite::Connection;
use serde_json::Value;
use snafu::prelude::*;
use snafu::{ensure, Snafu};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::PathBuf;
use tempfile::{tempfile_in, TempDir};
use derive_builder::Builder;

use arrow::csv::ReaderBuilder;
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

    #[snafu(display("{}", source))]
    RusqliteError { source: rusqlite::Error },

    #[snafu(display("{}", source))]
    JinjaError { source: minijinja::Error },

    #[snafu(display("{}", source))]
    ParquetError { source: ParquetError },

    #[snafu(display("{}", source))]
    ArrowError { source: ArrowError },
}


#[derive(Default, Builder, Debug)]
#[builder(setter(into))]
pub struct Options {
    pub delete_input_csv: bool
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
        .map(|field| match resource_fields.get(field) {
            Some(field) => Some(*field),
            None => None,
        })
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

enum CSVReaders {
    File((PathBuf, File)),
    Zip(zip::ZipArchive<File>),
}

fn get_csv_reader(file: &str, resource_path: &str) -> Result<CSVReaders, Error> {
    if file.ends_with(".json") {
        let mut file_pathbuf = PathBuf::from(file);
        file_pathbuf.pop();
        file_pathbuf.push(&resource_path);
        Ok(CSVReaders::File((file_pathbuf.clone(), File::open(&file_pathbuf).context(
            IoSnafu {
                filename: file_pathbuf.to_string_lossy()
            }
        )?)))
    } else if file.ends_with(".zip") {
        let zip_file = File::open(&file).context(IoSnafu {
            filename: file.clone(),
        })?;
        let zip = zip::ZipArchive::new(zip_file).context(ZipSnafu {
            filename: file.clone(),
        })?;
        Ok(CSVReaders::Zip(zip))
    } else if PathBuf::from(&file).is_dir() {
        let file_pathbuf = PathBuf::from(file);
        let file_pathbuf = file_pathbuf.join(&resource_path);
        Ok(CSVReaders::File((file_pathbuf.clone(), File::open(&file_pathbuf).context(
            IoSnafu {
                filename: file_pathbuf.to_string_lossy()
            }
        )?)))
    } else {
        Err(Error::DatapackageMergeError {
            message: "could not detect a datapackage".into(),
        })
    }
}

pub fn merge_datapackage(output_path: PathBuf, datapackages: Vec<String>) -> Result<(), Error> {
    let options = Options::default();
    merge_datapackage_with_options(output_path, datapackages, options)
}

pub fn merge_datapackage_with_options(mut output_path: PathBuf, datapackages: Vec<String>, options: Options) -> Result<(), Error> {
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

            let csv_readers = get_csv_reader(file, &resource_path)?;

            match csv_readers {
                CSVReaders::Zip(mut zip) => {
                    let zipped_file = zip
                        .by_name(&resource_path)
                        .context(ZipSnafu { filename: file })?;
                    let csv_reader = csv::Reader::from_reader(zipped_file);
                    csv_output =
                        write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields)?;
                }
                CSVReaders::File(file_reader) => {
                    let (filename, file_reader) = file_reader;
                    let csv_reader = csv::Reader::from_reader(file_reader);
                    csv_output =
                        write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields)?;
                    if options.delete_input_csv {
                        std::fs::remove_file(&filename).context(IoSnafu {filename: filename.to_string_lossy()})?;
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

pub fn to_sqlite_type(
    _state: &minijinja::State,
    value: String,
) -> Result<String, minijinja::Error> {
    let output = match value.as_str() {
        "string" => "TEXT".to_string(),
        "date" => "TIMESTAMP".to_string(),
        "number" => "NUMERIC".to_string(),
        "boolean" => "BOOLEAN".to_string(),
        _ => "TEXT".to_string(),
    };
    Ok(output)
}

fn render_sqlite_table(value: Value) -> Result<String, Error> {
    let sqlite_table = r#"
    CREATE TABLE [{{name}}] (
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
    let sqlite_table = sqlite_table.replace("\n", "");
    let sqlite_table = sqlite_table.replace("#nl", "\n");

    let mut env = Environment::new();
    env.add_filter("sqlite_type", to_sqlite_type);
    env.add_template("sqlite_resource", &sqlite_table).unwrap();
    let tmpl = env.get_template("sqlite_resource").unwrap();
    Ok(tmpl.render(value).context(JinjaSnafu {})?.to_owned())
}

fn insert_sql_data(
    csv_reader: csv::Reader<impl std::io::Read>,
    mut conn: rusqlite::Connection,
    resource: Value,
) -> Result<rusqlite::Connection, Error> {
    let tx = conn.transaction().context(RusqliteSnafu {})?;

    let table = resource["name"].as_str().unwrap();

    let mut fields = 0;

    if let Some(fields_vec) = resource["schema"]["fields"].as_array() {
        fields = fields_vec.len();
    };

    let mut question_marks = "?,".repeat(fields);

    question_marks.pop();

    {
        let mut statement = tx
            .prepare_cached(&format!("INSERT INTO [{table}] VALUES ({question_marks})"))
            .context(RusqliteSnafu {})?;

        for row in csv_reader.into_deserialize() {
            let this_row: Vec<String> = row.context(CSVSnafu { filename: table })?;

            statement
                .execute(rusqlite::params_from_iter(this_row.iter()))
                .context(RusqliteSnafu {})?;
        }
    }
    tx.commit().context(RusqliteSnafu {})?;
    return Ok(conn);
}


pub fn datapackage_to_sqlite(db_path: String, datapackage: String) -> Result<(), Error> {
    let options = Options::default();
    datapackage_to_sqlite_with_options(db_path, datapackage, options)
}


pub fn datapackage_to_sqlite_with_options(db_path: String, datapackage: String, options: Options) -> Result<(), Error> {
    let mut datapackage_value = datapackage_json_to_value(&datapackage)?;

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
        if let Some(name) = resource["name"].as_str() {
            if let Some(foreign_keys) = resource["schema"]["foreignKeys"].as_array() {
                for value in foreign_keys {
                    if let Some(foreign_key_table) = value["reference"]["resource"].as_str() {
                        table_links.push((foreign_key_table.to_owned(), name.to_owned()));
                    }
                }
            }
            table_links.push((name.to_owned(), name.to_owned()));
            table_to_schema.insert(name.to_owned(), resource.clone());
        }
    }

    let mut relationhip_graph = petgraph::graphmap::DiGraphMap::new();

    for (x, y) in table_links.iter() {
        relationhip_graph.add_edge(y, x, 1);
    }

    let ordered_tables = petgraph::algo::kosaraju_scc(&relationhip_graph);

    let mut conn = Connection::open(db_path).context(RusqliteSnafu {})?;

    conn.execute_batch(
        "PRAGMA journal_mode = OFF;
         PRAGMA synchronous = 0;
         PRAGMA locking_mode = EXCLUSIVE;
         PRAGMA temp_store = MEMORY;",
    )
    .context(RusqliteSnafu {})?;

    for tables in ordered_tables {
        for table in tables {
            let resource = table_to_schema.get(table).unwrap();

            let resource_sqlite = render_sqlite_table(resource.clone())?;

            conn.execute(&resource_sqlite, [])
                .context(RusqliteSnafu {})?;

            ensure!(
                resource["path"].is_string(),
                DatapackageMergeSnafu {
                    message: "Datapackages resources need a `path`"
                }
            );

            let resource_path = resource["path"].as_str().unwrap();

            let csv_readers = get_csv_reader(&datapackage, &resource_path)?;

            match csv_readers {
                CSVReaders::Zip(mut zip) => {
                    let zipped_file = zip.by_name(&resource_path).context(ZipSnafu {
                        filename: &datapackage,
                    })?;
                    let csv_reader = csv::Reader::from_reader(zipped_file);
                    conn = insert_sql_data(csv_reader, conn, resource.clone())?
                }
                CSVReaders::File(csv_file) => {
                    let (filename, file) = csv_file;
                    let csv_reader = csv::Reader::from_reader(file);
                    conn = insert_sql_data(csv_reader, conn, resource.clone())?;
                    if options.delete_input_csv {
                        std::fs::remove_file(&filename).context(IoSnafu {filename: filename.to_string_lossy()})?;
                    }
                }
            }
        }
    }

    Ok(())
}


fn create_parquet(
    file: impl std::io::Read + std::io::Seek,
    resource: Value,
    mut output_path: PathBuf,
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

    let arrow_csv_reader = ReaderBuilder::new()
        .with_schema(std::sync::Arc::new(Schema::new(arrow_fields)))
        .has_header(true)
        .build(file).context(ArrowSnafu {})?
        ;

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
    let options = Options::default();
    datapackage_to_parquet_with_options(output_path, datapackage, options)
}


pub fn datapackage_to_parquet_with_options(output_path: PathBuf, datapackage: String, options: Options) -> Result<(), Error> {
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

        let csv_readers = get_csv_reader(&datapackage, &resource_path)?;

        match csv_readers {
            CSVReaders::Zip(mut zip) => {
                let mut zipped_file = zip.by_name(&resource_path).context(ZipSnafu {
                    filename: &datapackage,
                })?;
                let mut tmp_csv = tempfile_in(&output_path).context(IoSnafu {
                    filename: output_path.to_string_lossy(),
                })?;
                std::io::copy(&mut zipped_file, &mut tmp_csv).context(IoSnafu {
                    filename: output_path.to_string_lossy(),
                })?;
                tmp_csv.seek(std::io::SeekFrom::Start(0)).context(IoSnafu {
                    filename: output_path.to_string_lossy(),
                })?;
                create_parquet(tmp_csv, resource.clone(), output_path.clone())?
            }
            CSVReaders::File(csv_reader) => {
                let (filename, file) = csv_reader;
                create_parquet(file, resource.clone(), output_path.clone())?;
                if options.delete_input_csv {
                    std::fs::remove_file(&filename).context(IoSnafu {filename: filename.to_string_lossy()})?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta;
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

            merge_datapackage(
                tmp.clone(),
                vec![
                    format!("fixtures/{datapackage1}/datapackage.json"),
                    format!("fixtures/{datapackage2}/datapackage.json"),
                ],
            )
            .unwrap();

            insta::assert_yaml_snapshot!(
                format!("{name}_json"),
                datapackage_json_to_value(&tmp.to_string_lossy().into_owned()).unwrap()
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
                datapackage_json_to_value(&tmp.to_string_lossy().into_owned()).unwrap()
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
                datapackage_json_to_value(&tmp.to_string_lossy().into_owned()).unwrap()
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

        let options = OptionsBuilder::default().delete_input_csv(true).build().unwrap();

        std::fs::copy("fixtures/add_resource/datapackage.json", tmp.join("datapackage.json")).unwrap();
        std::fs::create_dir_all(tmp.join("csv")).unwrap();
        std::fs::copy("fixtures/add_resource/csv/games.csv", tmp.join("csv/games.csv")).unwrap();
        std::fs::copy("fixtures/add_resource/csv/games2.csv", tmp.join("csv/games2.csv")).unwrap();

        datapackage_to_sqlite_with_options(
            tmp.join("sqlite.db").to_string_lossy().into(), 
            tmp.to_string_lossy().into(), 
            options).unwrap();
        
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

        let options = OptionsBuilder::default().delete_input_csv(true).build().unwrap();

        std::fs::copy("fixtures/add_resource/datapackage.json", tmp.join("datapackage.json")).unwrap();
        std::fs::create_dir_all(tmp.join("csv")).unwrap();
        std::fs::copy("fixtures/add_resource/csv/games.csv", tmp.join("csv/games.csv")).unwrap();
        std::fs::copy("fixtures/add_resource/csv/games2.csv", tmp.join("csv/games2.csv")).unwrap();

        datapackage_to_parquet_with_options(
            tmp.join("parquet"), 
            tmp.to_string_lossy().into(), 
            options).unwrap();
        
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
                for (idx, (name, field)) in row.get_column_iter().enumerate() {
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
}
