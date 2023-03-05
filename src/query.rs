use duckdb::Connection as DuckdbConnection;
use snafu::prelude::*;
use typed_builder::TypedBuilder;

#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{}", source))]
    DuckDbError { source: duckdb::Error },
}

#[derive(Default, Debug, TypedBuilder)]
pub struct Options {
    #[builder(default)]
    pub delimiter: String,
    #[builder(default)]
    pub quote: String,
    #[builder(default)]
    pub parquet: bool,
}


pub fn query(sql: String, output: String, options: Options) -> Result<(), Error> {
    let conn = DuckdbConnection::open_in_memory().context(DuckDbSnafu {})?;

    let sql = sql.trim();

    let sql = if let Some(sql) = sql.strip_suffix(";") {
        sql 
    } else {
        sql
    };
    
    let mut output_options = vec![];

    if options.parquet || output.ends_with(".parquet") {
        output_options.push("FORMAT 'parquet'".to_owned());
    } else {
        output_options.push("HEADER 1".to_owned());
        if !options.delimiter.is_empty() {
            output_options.push(format!("DELIMETER '{}'", options.delimiter));
        }
        if !options.quote.is_empty() {
            output_options.push(format!("QUOTE '{}'", options.quote));
        }
    }

    let with_part = output_options.join(", ");

    let output = if output == "-" {"/dev/stdout"} else {output.as_str()};

    let sql = format!("copy ({sql}) TO '{output}' WITH ({with_part}) ");

    conn.execute_batch("INSTALL parquet; LOAD parquet; INSTALL httpfs; LOAD httpfs;").context(DuckDbSnafu {})?;
    conn.execute_batch(&sql).context(DuckDbSnafu {})?;
    
    Ok(())
}


#[cfg(test)]
mod tests {
    use std::io::BufRead;

    use super::*;
    use insta::assert_debug_snapshot;
    use tempfile::TempDir;

    fn get_results(file: String) -> Vec<Vec<duckdb::types::Value>> {
        let conn = DuckdbConnection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL parquet; LOAD parquet").unwrap();
        let mut stmt = conn.prepare(&format!("select * from '{file}';")).unwrap();
        let mut rows = stmt.query([]).unwrap();

        let mut results = vec![];

        while let Some(row) = rows.next().unwrap() {
            let mut result_row = vec![];
            for i in 0.. {
                if let Ok(item) = row.get(i) {
                    let cell: duckdb::types::Value = item;
                    result_row.push(cell)
                } else {
                     break
                }
            }
            results.push(result_row)
        }
        results
    }

    #[test]
    fn test_query_to_parquet() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        let output: String = tmp.join("output.parquet").to_string_lossy().into();

        query(
            "select * from 'fixtures/add_resource/csv/games.csv'".into(),
            output.clone(),
            Options::builder().build()
        ).unwrap();

        let data = get_results(output);
        assert_debug_snapshot!(data);

        let output: String = tmp.join("output.parquet").to_string_lossy().into();

        query(
            "select * from 'fixtures/add_resource/csv/games.csv' where id=1".into(),
            output.clone(),
            Options::builder().parquet(true).build()
        ).unwrap();

        let data = get_results(output);
        assert_debug_snapshot!(data);

        let output: String = tmp.join("output.parquet").to_string_lossy().into();

        query(
            "select * from 'https://csvs-convert-test.s3.eu-west-1.amazonaws.com/games.parquet' where id=1".into(),
            output.clone(),
            Options::builder().parquet(true).build()
        ).unwrap();

        let data = get_results(output);
        assert_debug_snapshot!(data);

    }

    #[test]
    fn test_query_to_csvs() {
        let tmp_dir = TempDir::new().unwrap();
        let tmp = tmp_dir.path().to_owned();

        let output: String = tmp.join("output.csv").to_string_lossy().into();

        query(
            "select * from 'fixtures/add_resource/csv/games.csv' where id=1".into(),
            output.clone(),
            Options::builder().build()
        ).unwrap();

        let mut lines = vec![];

        for line in std::io::BufReader::new(std::fs::File::open(output).unwrap()).lines() {
            lines.push(line.unwrap())
        } 

        assert_debug_snapshot!(lines);

        let output: String = tmp.join("output.csv").to_string_lossy().into();

        query(
            "select * from 'fixtures/add_resource/csv/games.csv' where id=1".into(),
            output.clone(),
            Options::builder().delimiter(";".into()).build()
        ).unwrap();

        let mut lines = vec![];

        for line in std::io::BufReader::new(std::fs::File::open(output).unwrap()).lines() {
            lines.push(line.unwrap())
        } 

        assert_debug_snapshot!(lines);

        let output: String = tmp.join("output.csv").to_string_lossy().into();

        query(
            "select * from 'https://gist.githubusercontent.com/kindly/7937e7d707a8bc3c2c812b4c6c314dc1/raw/f5767b3a878fb87ebdbe0071f308f2a3e132c3b9/test.csv'".into(),
            output.clone(),
            Options::builder().build()
        ).unwrap();

        let mut lines = vec![];

        for line in std::io::BufReader::new(std::fs::File::open(output).unwrap()).lines() {
            lines.push(line.unwrap())
        } 

        assert_debug_snapshot!(lines);
        
    }

    // #[test]
    // fn test_s3() {
    //     query(
    //         "select * from 's3://csvs-convert-test/output.parquet'".into(),
    //         "s3://csvs-convert-test/output2.parquet".into(),
    //         Options::builder().build()
    //     ).unwrap();
    // }

}