use crate::describe::Describer;
use csv::Reader;
use serde_json::{json, Value};
use crossbeam_channel::bounded;
use std::thread::spawn;

pub fn describe(mut reader: Reader<std::fs::File>, with_stats:bool) -> Result<Value, csv::Error> {
    //let mut reader = csv::Reader::from_path(path).unwrap();

    let mut headers = vec![];
    let mut describers = vec![];
    {
        for header in reader.headers()? {
            headers.push(header.to_owned());
            let mut describer = Describer::new();
            describer.calculate_stats = with_stats;
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

        if with_stats {
            field.as_object_mut().expect("We know its an object").insert("stats".into(), describer.stats());
        }
        fields.push(field);
    }

    Ok(json!({"row_count": row_count, "fields": fields}))
}

//struct Sender {
//    channel: crossbeam_channel::Sender<(usize, String)>,
//} 

struct Receiver {
    cols: Vec<usize>,
    headers: Vec<String>,
    channel: crossbeam_channel::Receiver<(usize, String)>,
    describers: Vec<Describer>
} 

pub fn describe_parallel(mut reader: Reader<std::fs::File>, with_stats:bool, num_threads: usize) -> Value{
    //let mut reader = csv::Reader::from_path(path).unwrap();
    let mut senders = vec![];
    let mut receivers = vec![];

    for _ in 0_usize..num_threads {
        let (sender, receiver) = bounded(1000);
        senders.push(sender);
        receivers.push(Receiver {cols: vec![], headers: vec![], channel: receiver, describers: vec![]})
    }

    {
        for (col, header) in reader.headers().unwrap().iter().enumerate() {
            let mut describer = Describer::new();
            describer.calculate_stats = with_stats;
            receivers[col % num_threads].describers.push(describer);
            receivers[col % num_threads].headers.push(header.to_owned());
            receivers[col % num_threads].cols.push(col);
        }
    }

    let mut threads = vec![];

    for mut receiver in receivers {
        let thread = spawn(move || {
            for (i, item) in receiver.channel {
                receiver.describers[i].process(&item)
            }
            let mut fields = vec![];
            for (num, col) in receiver.cols.iter().enumerate() {
                let mut field = json!({
                    "name": receiver.headers[num],
                    "type": receiver.describers[num].guess_type().0,
                    "format": receiver.describers[num].guess_type().1,
                });

                if with_stats {
                    field.as_object_mut().unwrap().insert("stats".into(), receiver.describers[num].stats());
                }
                fields.push((*col, field))
            }
            fields
        });
        threads.push(thread);
    } 


    let mut row_count: usize = 0;

    for row in reader.records() {
        let record = row.unwrap();

        for (index, cell) in record.into_iter().enumerate() {
            senders[index % num_threads].send((index.div_euclid(num_threads), cell.to_owned())).expect("should send");
        }
        row_count += 1;
    }
    senders.clear();

    let mut fields = vec![];
    for thread in threads {
        let field = thread.join().expect("should join");
        fields.extend(field);
    }
    fields.sort_by_key(|(a, _)| {*a});
    let fields: Vec<Value> = fields.into_iter().map(|i| {i.1}).collect();

    json!({"row_count": row_count, "fields": fields})
}


pub fn describe_parallel_rows(mut reader: Reader<std::fs::File>, with_stats:bool) -> Value{

    let mut headers = vec![];
    {
        for header in reader.headers().unwrap() {
            headers.push(header.to_owned());
        }
    }

    let (send, receive) = bounded(1000);

    let thread = spawn(move|| {
        for row in reader.records() {
            let record = row.unwrap();
            send.send(record).unwrap();
        }
    });

    let mut describers = vec![];
    for _ in headers.iter() {
        let mut describer = Describer::new();
        describer.calculate_stats = with_stats;
        describers.push(describer)
    }

    let mut row_count: usize = 0;
    for record in receive {
        for (index, cell) in record.iter().enumerate() {
            describers[index].process(cell);
        }
        row_count += 1;
    }

    thread.join().unwrap();

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
    use crate::describe_csv::{describe, describe_parallel};

    #[test]
    fn all_types() {
        let reader = csv::Reader::from_path("src/fixtures/all_types.csv").unwrap();
        let metadata = describe(reader, false);

        insta::assert_yaml_snapshot!(metadata.unwrap());
    }

    #[test]
    fn large_multi() {
        let reader = csv::Reader::from_path("fixtures/large/csv/data.csv").unwrap();
        let metadata = describe_parallel(reader, true, 2);

        let reader = csv::Reader::from_path("fixtures/large/csv/data.csv").unwrap();
        let metadata_multi = describe(reader, true);

        assert_eq!(metadata, metadata_multi);

        insta::assert_yaml_snapshot!(metadata);
    }

}
