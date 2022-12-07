use crate::describe::Describer;
use csv::Reader;
use serde_json::{json, Value};

pub fn describe(mut reader: Reader<std::fs::File>, with_stats:bool) -> Value{
    //let mut reader = csv::Reader::from_path(path).unwrap();

    let mut headers = vec![];
    let mut describers = vec![];
    {
        for header in reader.headers().unwrap() {
            headers.push(header.to_owned());
            let mut describer = Describer::new();
            describer.calculate_stats = with_stats;
            describers.push(Describer::new())
        }
    }

    let mut row_count: usize = 0;

    for row in reader.records() {
        let record = row.unwrap();
        for (index, cell) in record.iter().enumerate() {
            describers[index].process(cell);
        }
        row_count += 1;
    }

    let mut fields = vec![];
    for (num, mut describer) in describers.into_iter().enumerate() {

        let mut field = json!({
            "name": headers[num],
            "type": describer.guess_type().0,
            "format": describer.guess_type().1,
        });

        if with_stats {
            field.as_object_mut().unwrap().insert("stats".into(), describer.stats());
        }
        fields.push(field);
    }

    json!({"row_count": row_count, "fields": fields})
}

#[cfg(test)]
mod tests {
    use crate::describe_csv::describe;

    #[test]
    fn all_types() {
        let reader = csv::Reader::from_path("src/fixtures/all_types.csv").unwrap();
        let metadata = describe(reader, false);

        insta::assert_yaml_snapshot!(metadata);
    }
}