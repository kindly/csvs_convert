use snafu::{Snafu, ensure};
use snafu::prelude::*;
use serde_json::Value;
use std::fs::File;
use std::path::PathBuf;
use std::io::BufReader;
use csv::Writer;

#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{}", message))]
    DatapackageMergeError { message: String },

    #[snafu(display("Error reading file {}: {}", filename, source))]
    IoError { source: std::io::Error, filename: String },

    #[snafu(display("Error parsing JSON {}: {}", filename, source))]
    JSONError { source: serde_json::Error, filename: String },

    #[snafu(display("Error loading zip file {}: {}", filename, source))]
    ZipError { source: zip::result::ZipError, filename: String },

    #[snafu(display("Error loading zip file {}: {}", filename, source))]
    CSVError { source: csv::Error, filename: String }
}

fn make_mergeable_resource(mut resource: Value) -> Result<Value, Error>{
    let mut fields = resource["schema"]["fields"].take();
    let fields_option = fields.as_array_mut();

    ensure!(fields_option.is_some(), DatapackageMergeSnafu {message: "Datapackages need a `fields` list"});

    let mut new_fields = serde_json::Map::new();
    for field in fields_option.unwrap().drain(..) {
        let name_option = field["name"].as_str();
        ensure!(name_option.is_some(), DatapackageMergeSnafu {message: "Each field needs a name"});
        new_fields.insert(name_option.unwrap().to_owned(), field);
    };

    resource["schema"].as_object_mut().unwrap().insert("fields".to_string(), new_fields.into());

    Ok(resource)
}

fn make_mergeable_datapackage(mut value: Value) -> Result<Value, Error> {

    let mut resources = value["resources"].take();

    let resources_option = resources.as_array_mut();
    ensure!(resources_option.is_some(), DatapackageMergeSnafu {message: "Datapackages need a `resources` key as an array"});

    let mut new_resources = serde_json::Map::new();
    for resource in resources_option.unwrap().drain(..) {
        let path;
        {
            let path_str = resource["path"].as_str();
            ensure!(path_str.is_some(), DatapackageMergeSnafu{message: "datapackage resource needs a name or path"});
            path = path_str.unwrap().to_owned();
        }

        let new_resource = make_mergeable_resource(resource)?;
        new_resources.insert(path, new_resource);
    };

    value.as_object_mut().unwrap().insert("resources".into(), new_resources.into());

    Ok(value)
}


fn make_datapackage_from_mergeable(mut value: Value) -> Result<Value, Error> {

    let mut resources = value["resources"].take();

    let resources_option = resources.as_object_mut();

    let mut new_resources = vec![];
    for resource in resources_option.unwrap().values_mut() {
        let new_resource = make_resource_from_mergable(resource.clone())?;
        new_resources.push(new_resource);
    };

    value.as_object_mut().unwrap().insert("resources".into(), new_resources.into());

    Ok(value)
}

fn make_resource_from_mergable(mut resource: Value) -> Result<Value, Error>{
    let mut fields = resource["schema"]["fields"].take();
    let fields_option = fields.as_object_mut();

    let mut new_fields = vec![];
    for field in fields_option.unwrap().values_mut() {
        new_fields.push(field.clone());
    };

    resource["schema"].as_object_mut().unwrap().insert("fields".to_string(), new_fields.into());

    Ok(resource)
}


fn datapackage_json_to_value (filename: &str) -> Result<Value, Error> {
    if filename.ends_with(".json") {
        let file = File::open(&filename).context(IoSnafu {filename})?;
        let json: Value = serde_json::from_reader(BufReader::new(file)).context(JSONSnafu{filename})?;
        Ok(json)
    } else if filename.ends_with(".zip") {
        let file = File::open(&filename).context(IoSnafu {filename})?;
        let mut zip = zip::ZipArchive::new(file).context(ZipSnafu {filename})?;
        let zipped_file = zip.by_name("datapackage.json").context(ZipSnafu {filename})?;
        let json: Value = serde_json::from_reader(BufReader::new(zipped_file)).context(JSONSnafu{filename})?;
        Ok(json)
    } else if PathBuf::from(filename).is_dir() {
        let mut path = PathBuf::from(filename);
        path.push("datapackage.json");
        let file = File::open(path).context(IoSnafu {filename})?;
        let json: Value = serde_json::from_reader(BufReader::new(file)).context(JSONSnafu{filename})?;
        Ok(json)
    } else {
        Err(Error::DatapackageMergeError {message: "could not detect a datapackage".into()})
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
                ensure!(field_value.is_object(), DatapackageMergeSnafu {message: "Each field needs to be an object"});

                let base_fields = base_resources[resource]["schema"]["fields"].as_object_mut().unwrap();

                if !base_fields.contains_key(field) {
                    base_fields.insert(field.clone(), field_value.clone());
                } else {
                    ensure!(base_fields[field].is_object(), DatapackageMergeSnafu {message: "Each field needs to be an object"});
                    let base_fieldinfo = base_fields[field].as_object_mut().unwrap();

                    let base_type = base_fieldinfo["type"].as_str().unwrap_or_default();
                    let field_type = field_value["type"].as_str().unwrap_or_default();

                    if field_type != base_type || base_type.is_empty() || field_type.is_empty() {
                        base_fieldinfo.insert("type".to_string(), "string".into());
                    }

                    let base_count = base_fieldinfo["count"].as_u64().unwrap_or_default();
                    let field_count = field_value["count"].as_u64().unwrap_or_default();
                    
                    if base_count > 0 && field_count > 0 {
                        base_fieldinfo.insert("count".to_string(), (field_count + base_count).into());
                    }
                }
            }
        }
    }
    Ok(base)
}


pub fn merge_datapackage_jsons(datapackages: Vec<String>) -> Result<Value, Error> {
    ensure!(datapackages.len() > 1, DatapackageMergeSnafu {message: "Need more 2 or more datapackages"});
    let mut merged_value = make_mergeable_datapackage(datapackage_json_to_value(&datapackages[0])?)?;

    for file in datapackages[1..].iter() {
        merged_value = merge_datapackage_json(merged_value, make_mergeable_datapackage(datapackage_json_to_value(file)?)?)?;
    }

    make_datapackage_from_mergeable(merged_value)
}


pub fn write_merged_csv(
    csv_reader: csv::Reader<impl std::io::Read>, 
    mut csv_writer: Writer<File>,
    resource_fields: &std::collections::HashMap<String, usize>,
    output_fields: &[String]
) -> Result<Writer<File>, Error> {
    let output_map: Vec<Option<&usize>> = output_fields.iter().map(|field| {resource_fields.get(field)}).collect();
    let output_map_len = output_map.len();
    for row in csv_reader.into_records() {
        let mut output_row = Vec::with_capacity(output_map_len);
        let row = row.unwrap();
        for item in &output_map {
            match item {
                Some(index) => {output_row.push(row.get(**index).unwrap())},
                None => {output_row.push("")}
            }
        }
        csv_writer.write_record(output_row).unwrap();
    }
    Ok(csv_writer)
}


pub fn merge_datapackage(datapackages: Vec<String>, output_path: PathBuf) -> Result<(), Error> {
    ensure!(datapackages.len() > 1, DatapackageMergeSnafu {message: "Need more 2 or more files"});
    
    std::fs::create_dir_all(&output_path).context(IoSnafu {filename: output_path.to_string_lossy()})?;

    let mut merged_datapackage_json = merge_datapackage_jsons(datapackages.clone())?;

    let path = PathBuf::from(&output_path);

    let datapackage_json_path_buf = path.join("datapackage.json");

    let writer = File::create(&datapackage_json_path_buf).context(IoSnafu {filename: datapackage_json_path_buf.to_string_lossy()})?;

    serde_json::to_writer_pretty(writer, &merged_datapackage_json).context(JSONSnafu {filename: datapackage_json_path_buf.to_string_lossy()})?;

    let mut csv_outputs = std::collections::HashMap::new();
    let mut output_fields = std::collections::HashMap::new();

    for resource in merged_datapackage_json["resources"].as_array_mut().unwrap() {

        let mut field_order_map = serde_json::Map::new();
        let mut fields: Vec<String> = Vec::new();
        for (index, field) in resource["schema"]["fields"].as_array().unwrap().iter().enumerate() {
            let name = field["name"].as_str().unwrap();
            field_order_map.insert(name.into(), index.into());
            fields.push(name.to_owned());
        }

        let resource_path = resource["path"].as_str().unwrap().to_owned();

        let mut full_path = path.join(&resource_path);
        full_path.pop();
        std::fs::create_dir_all(&full_path).context(IoSnafu {filename: full_path.to_string_lossy()})?;

        let mut writer = Writer::from_path(path.join(&resource_path)).context(CSVSnafu {filename: &resource_path})?;
        writer.write_record(fields.clone()).context(CSVSnafu {filename: &resource_path})?;
        csv_outputs.insert(resource_path.clone(), writer);

        output_fields.insert(resource_path.clone(), fields);

        resource.as_object_mut().unwrap().insert("field_order_map".into(), field_order_map.into());
    }

    for file in datapackages.iter() {
        let mut datapackage_json = datapackage_json_to_value(file)?;
        for resource in datapackage_json["resources"].as_array_mut().unwrap() {
            let mut resource_fields = std::collections::HashMap::new();
            for (num, field) in resource["schema"]["fields"].as_array().unwrap().iter().enumerate() {
                resource_fields.insert(field["name"].as_str().unwrap().to_owned(), num);
            }
            let resource_path = resource["path"].as_str().unwrap().to_owned();

            let mut csv_output = csv_outputs.remove(&resource_path).unwrap();

            let output_fields = output_fields.get_mut(&resource_path).unwrap();

            if file.ends_with(".json") {
                let mut file_pathbuf = PathBuf::from(file);
                file_pathbuf.pop();
                file_pathbuf.push(&resource_path);
                let csv_reader = csv::Reader::from_path(file_pathbuf).unwrap();
                csv_output = write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields).unwrap();
            } else if file.ends_with(".zip") {
                let zip_file = File::open(&file).context(IoSnafu {filename: file})?;
                let mut zip = zip::ZipArchive::new(zip_file).context(ZipSnafu {filename: file})?;
                let zipped_file = zip.by_name(&resource_path).context(ZipSnafu {filename: file})?;
                let csv_reader = csv::Reader::from_reader(zipped_file);
                csv_output = write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields).unwrap();
            } else if PathBuf::from(file).is_dir() {
                let path = PathBuf::from(file);
                let path = path.join(&resource_path);
                let csv_reader = csv::Reader::from_reader(File::open(path).unwrap());
                csv_output = write_merged_csv(csv_reader, csv_output, &resource_fields, output_fields).unwrap();
            } else {
                return Err(Error::DatapackageMergeError {message: "could not detect a datapackage".into()})
            }

            csv_outputs.insert(resource_path, csv_output);
        }
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use insta;
    use tempfile::TempDir;
    use std::io::BufRead;

    fn test_merged_csv_output(tmp: &PathBuf, name: String) {
        let csv_dir = tmp.join("csv");
        let paths = std::fs::read_dir(csv_dir).unwrap();
        for path in paths {
            let path = path.unwrap().path();
            let file_name = path.file_name().unwrap().to_string_lossy().into_owned();
            let test_name = format!("{name}_{file_name}");
            let file = File::open(path).unwrap();
            let lines: Vec<String> = std::io::BufReader::new(file).lines().map(|x| x.unwrap()).collect();
            insta::assert_yaml_snapshot!(test_name, lines);
        }

    }

    fn test_datapackage_merge(name: &str, datapackage1: &str, datapackage2: &str) {
        {
            let tmp_dir = TempDir::new().unwrap();
            let tmp = tmp_dir.path().to_owned();

            merge_datapackage(
                    vec![format!("fixtures/{datapackage1}/datapackage.json"), format!("fixtures/{datapackage2}/datapackage.json")],
                    tmp.clone()
            ).unwrap();

            insta::assert_yaml_snapshot!(format!("{name}_json"),
                datapackage_json_to_value(&tmp.to_string_lossy().into_owned()).unwrap()
            );
            test_merged_csv_output(&tmp, format!("{name}_json"))
        }

        {
            let temp_dir = TempDir::new().unwrap();
            let tmp = temp_dir.path().to_path_buf();

            merge_datapackage(
                    vec![format!("fixtures/{datapackage1}"), format!("fixtures/{datapackage2}")],
                    tmp.clone()
            ).unwrap();

            insta::assert_yaml_snapshot!(format!("{name}_folder"),
                datapackage_json_to_value(&tmp.to_string_lossy().into_owned()).unwrap()
            );
            test_merged_csv_output(&tmp, format!("{name}_folder"))
        }

        {
            let temp_dir = TempDir::new().unwrap();
            let tmp = temp_dir.path().to_path_buf();

            merge_datapackage(
                    vec![format!("fixtures/{datapackage1}.zip"), format!("fixtures/{datapackage2}.zip")],
                    tmp.clone()
            ).unwrap();

            insta::assert_yaml_snapshot!(format!("{name}_zip"),
                datapackage_json_to_value(&tmp.to_string_lossy().into_owned()).unwrap()
            );
            test_merged_csv_output(&tmp, format!("{name}_zip"))
        }
    }

    #[test]
    fn test_datapackage_merge_self() {
        test_datapackage_merge("base","base_datapackage", "base_datapackage");
    }

    #[test]
    fn test_datapackage_add_resource() {
        test_datapackage_merge("add_resource","base_datapackage", "add_resource");
    }

    #[test]
    fn test_datapackage_add_different_resource() {
        test_datapackage_merge("add_different_resource","base_datapackage", "add_different_resource");
    }

    #[test]
    fn test_datapackage_add_field() {
        test_datapackage_merge("add_field","base_datapackage", "add_field");
    }

    #[test]
    fn test_conflict_types() {
        test_datapackage_merge("conflict_types","base_datapackage", "conflict_types");
    }

    #[test]
    fn test_multiple() {
        insta::assert_yaml_snapshot!(merge_datapackage_jsons(
            vec!["fixtures/base_datapackage/datapackage.json".into(),
                 "fixtures/base_datapackage/datapackage.json".into(),
                 "fixtures/add_resource/datapackage.json".into(),
                 "fixtures/add_different_resource/datapackage.json".into(),
                 "fixtures/add_field/datapackage.json".into(),
                 "fixtures/conflict_types/datapackage.json".into()]
                ).unwrap());
    }

}