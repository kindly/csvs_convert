use snafu::{Snafu, ensure};
use snafu::prelude::*;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;

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
    ZipError { source: zip::result::ZipError, filename: String }
}

fn get_name_or_path(value: &Value) -> Result<String, Error> {
    let mut name;
    
    name = value["name"].as_str();
    if name.is_none() {
        name = value["path"].as_str()
    }
    ensure!(!name.is_none(), DatapackageMergeSnafu{message: "datapackage resource needs a name or path"});

    return Ok(name.unwrap().to_owned())
}


fn make_mergeable_resource(mut resource: Value) -> Result<Value, Error>{
    let mut fields = resource["schema"]["fields"].take();
    let fields_option = fields.as_array_mut();

    ensure!(!fields_option.is_none(), DatapackageMergeSnafu {message: "Datapackages need a `fields` list"});

    let mut new_fields = serde_json::Map::new();
    for field in fields_option.unwrap().drain(..) {
        let name_option = field["name"].as_str();
        ensure!(!name_option.is_none(), DatapackageMergeSnafu {message: "Each field needs a name"});
        new_fields.insert(name_option.unwrap().to_owned(), field);
    };

    resource["schema"].as_object_mut().unwrap().insert("fields".to_string(), new_fields.into());

    return Ok(resource)
}

fn make_mergeable_datapackage(mut value: Value) -> Result<Value, Error> {

    let mut resources = value["resources"].take();

    let resources_option = resources.as_array_mut();
    ensure!(!resources_option.is_none(), DatapackageMergeSnafu {message: "Datapackages need a `resources` key as an array"});

    let mut new_resources = serde_json::Map::new();
    for resource in resources_option.unwrap().drain(..) {
        let name = get_name_or_path(&resource)?;
        let new_resource = make_mergeable_resource(resource)?;
        new_resources.insert(name, new_resource);
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

    return Ok(resource)
}


fn file_to_value (filename: &str) -> Result<Value, Error> {
    if filename.ends_with(".json") {
        let file = File::open(&filename).context(IoSnafu {filename: filename})?;
        let json: Value = serde_json::from_reader(BufReader::new(file)).context(JSONSnafu{filename: filename})?;
        Ok(json)
    } else if filename.ends_with(".zip") {
        let file = File::open(&filename).context(IoSnafu {filename: filename})?;
        let mut zip = zip::ZipArchive::new(file).context(ZipSnafu {filename: filename})?;
        let zipped_file = zip.by_name("datapackage.json").context(ZipSnafu {filename: filename})?;
        let json: Value = serde_json::from_reader(BufReader::new(zipped_file)).context(JSONSnafu{filename: filename})?;
        Ok(json)
    } else if std::path::PathBuf::from(filename).is_dir() {
        let mut path = std::path::PathBuf::from(filename);
        path.push("datapackage.json");
        let file = File::open(path).context(IoSnafu {filename: filename})?;
        let json: Value = serde_json::from_reader(BufReader::new(file)).context(JSONSnafu{filename: filename})?;
        Ok(json)
    } else {
        Err(Error::DatapackageMergeError {message: "could not detect a datapackage".into()})
    }
}

pub fn merge_datapackage_json(files: Vec<String>) -> Result<Value, Error> {
    ensure!(files.len() > 1, DatapackageMergeSnafu {message: "Need more 2 or more files"});
    let mut merged_value = make_mergeable_datapackage(file_to_value(&files[0])?)?;

    for file in files[1..].iter() {
        merged_value = merge_datapackage(merged_value, make_mergeable_datapackage(file_to_value(file)?)?)?;
    }

    Ok(make_datapackage_from_mergeable(merged_value)?)
}

fn merge_datapackage(mut base: Value, mut merger: Value) -> Result<Value, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use insta;

    fn test_datapackage_merge(name: &str, datapackage1: &str, datapackage2: &str) {
        insta::assert_yaml_snapshot!(format!("{name}_json"),
            merge_datapackage_json(
                vec![format!("fixtures/{datapackage1}.json"), format!("fixtures/{datapackage2}.json")]
            ).unwrap()
        );
        insta::assert_yaml_snapshot!(format!("{name}_folder"),
            merge_datapackage_json(
                vec![format!("fixtures/{datapackage1}"), format!("fixtures/{datapackage2}")]
            ).unwrap()
        );
        insta::assert_yaml_snapshot!(format!("{name}_zip"),
            merge_datapackage_json(
                vec![format!("fixtures/{datapackage1}.zip"), format!("fixtures/{datapackage2}.zip")]
            ).unwrap()
        );
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
        insta::assert_yaml_snapshot!(merge_datapackage_json(
            vec!["fixtures/base_datapackage.json".into(),
                 "fixtures/base_datapackage.json".into(),
                 "fixtures/add_resource.json".into(),
                 "fixtures/add_different_resource.json".into(),
                 "fixtures/add_field.json".into(),
                 "fixtures/conflict_types.json".into()]
                ).unwrap());
    }
}