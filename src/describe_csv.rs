use crate::describe::Describer;
use crate::describer::Options;
use crossbeam_channel::unbounded;
use csv::Reader;
use serde_json::{json, Value};
use std::path::PathBuf;

pub fn describe(mut reader: Reader<std::fs::File>, options: Options) -> Result<Value, csv::Error> {
    //let mut reader = csv::Reader::from_path(path).unwrap();

    let mut headers = vec![];
    let mut describers = vec![];
    {
        for header in reader.headers()? {
            headers.push(header.to_owned());
            let describer = Describer::new_with_options(options.clone());
            describers.push(describer)
        }
    }

    let mut row_count: usize = 0;

    for row in reader.records() {
        let record = row?;
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

        if options.stats || options.mergable_stats {
            field
                .as_object_mut()
                .expect("We know its an object")
                .insert("stats".into(), describer.stats());
        }
        fields.push(field);
    }

    Ok(json!({"row_count": row_count, "fields": fields}))
}

//struct Sender {
//    channel: crossbeam_channel::Sender<(usize, String)>,
//}

pub fn describe_parallel(
    reader_builder: csv::ReaderBuilder,
    file: PathBuf,
    options: Options,
    num_threads: usize,
) -> Result<Value, csv::Error> {
    let mut wtr = std::io::Cursor::new(vec![]);

    {
        csv_index::RandomAccessSimple::create(
            &mut reader_builder.from_path(file.clone())?,
            &mut wtr,
        )?;
    }

    let mut idx = csv_index::RandomAccessSimple::open(wtr)?;

    let pool = threadpool::ThreadPool::new(num_threads);

    let mut reader = reader_builder.from_path(file.clone())?;

    let mut headers = vec![];
    {
        for header in reader.headers()? {
            headers.push(header.to_owned());
        }
    }

    let (send, receive) = unbounded();

    let chunk_size = std::cmp::max((idx.len() as usize) / num_threads, 1);
    let mut current_index = 1;

    loop {
        if idx.len() <= current_index {
            break;
        }
        let headers_clone = headers.clone();
        let send_clone = send.clone();
        let options_clone = options.clone();
        let pos = idx.get(current_index)?;
        let mut reader = reader_builder.from_path(file.clone())?;
        reader.seek(pos).unwrap();

        pool.execute(move || {
            let mut describers = vec![];
            for _ in headers_clone.iter() {
                let describer = Describer::new_with_options(options_clone.clone());
                describers.push(describer)
            }

            for row in reader.records().into_iter().take(chunk_size) {
                let record = match row {
                    Ok(record) => record,
                    Err(error) => {
                        send_clone
                            .send(Err(error))
                            .expect("channel sending should work");
                        panic!()
                    }
                };
                for (index, cell) in record.iter().enumerate() {
                    describers[index].process(cell);
                }
            }
            send_clone
                .send(Ok(describers))
                .expect("channel should be there");
        });

        current_index += chunk_size as u64;
    }
    pool.join();
    drop(send);

    let mut all_describers = vec![];

    for describers in receive {
        let describers = describers?;
        if all_describers.is_empty() {
            for describer in describers.into_iter() {
                all_describers.push(describer)
            }
            continue;
        }

        for (num, describer) in describers.into_iter().enumerate() {
            all_describers[num].merge(describer)
        }
    }

    let mut fields = vec![];
    for (num, mut describer) in all_describers.into_iter().enumerate() {
        let mut field = json!({
            "name": headers[num],
            "type": describer.guess_type().0,
            "format": describer.guess_type().1,
        });

        if options.stats || options.mergable_stats {
            field
                .as_object_mut()
                .expect("just main field above")
                .insert("stats".into(), describer.stats());
        }
        fields.push(field);
    }

    return Ok(json!({"row_count": idx.len() - 1,"fields": fields}));
}

#[cfg(test)]
mod tests {
    use crate::describe_csv::{describe, describe_parallel, Options};

    #[test]
    fn all_types() {
        let reader = csv::Reader::from_path("src/fixtures/all_types.csv").unwrap();
        let metadata = describe(reader, Options::builder().build());

        insta::assert_yaml_snapshot!(metadata.unwrap());
    }

    #[test]
    fn large_multi() {
        let reader_builder = csv::ReaderBuilder::new();
        let metadata_multi = describe_parallel(
            reader_builder,
            "fixtures/large/csv/data.csv".into(),
            Options::builder().build(),
            8,
        )
        .unwrap();

        let reader = csv::Reader::from_path("fixtures/large/csv/data.csv").unwrap();
        let metadata = describe(reader, Options::builder().build()).unwrap();

        assert_eq!(metadata.clone(), metadata_multi);

        insta::assert_yaml_snapshot!(metadata);
    }
}
