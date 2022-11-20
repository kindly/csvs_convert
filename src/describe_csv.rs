use crate::describe::Describer;
use csv::Reader;
use serde_json::{json, Value};

pub fn describe(mut reader: Reader<std::fs::File>) -> Value{
    //let mut reader = csv::Reader::from_path(path).unwrap();

    let mut headers = vec![];
    let mut describers = vec![];
    {
        for header in reader.headers().unwrap() {
            headers.push(header.to_owned());
            describers.push(Describer::new())
        }
    }


    for row in reader.records() {
        let record = row.unwrap();
        for (index, cell) in record.iter().enumerate() {
            describers[index].process(cell);
        }
    }

    let mut fields = vec![];
    for (num, mut describer) in describers.into_iter().enumerate() {
        fields.push(
            json!({
                "name": headers[num],
                "type": describer.guess_type().0,
                "format": describer.guess_type().1,
            })
        )
    }

    fields.into()
}

#[cfg(test)]
mod tests {
    use crate::describe_csv::describe;

    #[test]
    fn all_types() {
        let reader = csv::Reader::from_path("src/fixtures/all_types.csv").unwrap();
        let metadata = describe(reader);

        insta::assert_yaml_snapshot!(metadata);
    }

}