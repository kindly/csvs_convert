use pdatastructs::hyperloglog::HyperLogLog;
use pdatastructs::num_traits::ToPrimitive;
use pdatastructs::tdigest;
use stats::OnlineStats;
use std::cmp::{max, min};
use std::collections::HashSet;
use typed_builder::TypedBuilder;

use chrono::prelude::*;
use chrono::DateTime;

use serde_json::json;

fn descriptions() -> Vec<(&'static str, &'static str)> {
    let mut output = vec![
        ("boolean", "boolean"),
        ("integer", "integer"),
        ("number", "number"),
        ("array", "array"),
        ("object", "object"),
        ("datetime_tz", "rfc2822"),
        ("datetime_tz", "rfc3339"),
    ];

    for datetime_format in datetime_formats() {
        output.push(("datetime", datetime_format))
    }

    for datetime_format in datetime_tz_formats() {
        output.push(("datetime_tz", datetime_format))
    }

    for date_format in date_formats() {
        output.push(("date", date_format))
    }

    for time_format in time_formats() {
        output.push(("time", time_format))
    }

    output
}

fn datetime_formats() -> Vec<&'static str> {
    vec![
        //"%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %I:%M:%S %p",
        "%Y-%m-%d %I:%M %p",
        "%Y %b %d %H:%M:%S",
        "%B %d %Y %H:%M:%S",
        "%B %d %Y %I:%M:%S %p",
        "%B %d %Y %I:%M %p",
        "%Y %b %d at %I:%M %p",
        "%d %B %Y %H:%M:%p",
        "%d %B %Y %H:%M",
        "%d %B %Y %H:%M:%S.%f",
        "%d %B %Y %I:%M:%S %p",
        "%d %B %Y %I:%M %p",
        "%B %d %Y %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%y %H:%M:%S.%f",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%y %I:%M %p",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S.%f",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d/%m/%y %H:%M:%S.%f",
        "%d/%m/%y %I:%M:%S %p",
        "%d/%m/%y %I:%M %p",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S.%f",
        "%d/%m/%Y %I:%M:%S %p",
        "%d/%m/%Y %I:%M %p",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y/%m/%d %I:%M:%S %p",
        "%Y/%m/%d %I:%M %p",
        "%y%m%d %H:%M:%S",
    ]
}

fn datetime_tz_formats() -> Vec<&'static str> {
    vec![
        //"%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M%#z",
        "%Y-%m-%d %H:%M:%S%.fZ",
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S.%f %Z",
        "%B %d %Y %H:%M:%S %Z",
        "%B %d %Y %H:%M %Z",
        "%B %d %Y %I:%M:%S %p %Z",
        "%B %d %Y %I:%M %p %Z",
    ]
}

fn date_formats() -> Vec<&'static str> {
    vec![
        "%Y-%m-%d", "%Y-%b-%d", "%B %d %y", "%B %d %Y", "%d %B %y", "%d %B %Y", "%m/%d/%y",
        "%m/%d/%Y", "%d/%m/%y", "%d/%m/%Y", "%Y/%m/%d", "%m.%d.%Y", "%Y.%m.%d",
    ]
}

fn time_formats() -> Vec<&'static str> {
    vec!["%H:%M", "%I:%M:%S %p", "%I:%M %p"]
}

#[derive(Default, Debug, TypedBuilder, Clone)]
pub struct Options {
    #[builder(default)]
    pub stats: bool,
    #[builder(default)]
    pub mergable_stats: bool,
    #[builder(default)]
    pub force_string: bool,
}

#[derive(Debug)]
pub struct Describer {
    pub count: usize,
    pub empty_count: usize,
    descriptions: Vec<(&'static str, &'static str)>,
    pub options: Options,
    to_delete: Vec<usize>,
    no_string_stats: bool,
    pub unique_to_large: bool,
    pub string_freq: counter::Counter<String>,
    pub max_len: Option<usize>,
    pub min_len: Option<usize>,
    pub max_number: Option<f64>,
    pub min_number: Option<f64>,
    pub sum: f64,
    pub minmax_str: stats::MinMax<Vec<u8>>,
    pub loglog: HyperLogLog<str>,
    pub tdigest: tdigest::TDigest<tdigest::K1>,
    pub stats: OnlineStats,
}

impl Describer {
    pub fn new() -> Describer {
        let options = Options::builder().build();
        Describer::new_with_options(options)
    }

    pub fn new_with_options(options: Options) -> Describer {
        let scale_function = tdigest::K1::new(100.into());
        let descriptions = if options.force_string {
            vec![]
        } else {
            descriptions()
        };
        return Describer {
            count: 0,
            empty_count: 0,
            descriptions,
            to_delete: vec![],
            options,
            no_string_stats: false,
            unique_to_large: false,
            string_freq: counter::Counter::new(),
            max_len: None,
            min_len: None,
            max_number: None,
            min_number: None,
            sum: 0_f64,
            minmax_str: stats::MinMax::new(),
            loglog: HyperLogLog::new(12),
            tdigest: tdigest::TDigest::new(scale_function, 1000),
            stats: OnlineStats::new(),
        };
    }

    pub fn merge(&mut self, other: Describer) {
        if self.options.mergable_stats {
            self.count += other.count;
            self.empty_count += other.empty_count;
            self.max_len = max(self.max_len, other.max_len);
            self.min_len = match (self.min_len, other.min_len) {
                (Some(x), Some(y)) => Some(min(x, y)),
                (None, Some(y)) => Some(y),
                _ => self.min_len,
            };

            if other.count > 0 {
                self.minmax_str.add(
                    other
                        .minmax_str
                        .min()
                        .expect("checked for not sample")
                        .clone(),
                );
                self.minmax_str.add(
                    other
                        .minmax_str
                        .max()
                        .expect("checked for not sample")
                        .clone(),
                );
            }
            self.loglog.merge(&other.loglog);
            self.sum += other.sum;

            self.max_number = match (self.max_number, other.max_number) {
                (Some(x), Some(y)) => Some(x.max(y)),
                (None, Some(y)) => Some(y),
                _ => self.max_number,
            };

            self.min_number = match (self.min_number, other.min_number) {
                (Some(x), Some(y)) => Some(x.min(y)),
                (None, Some(y)) => Some(y),
                _ => self.min_number,
            };
        }

        let self_desc: HashSet<_> = self.descriptions.iter().collect();
        let other_desc: HashSet<_> = other.descriptions.iter().collect();
        self.descriptions = self_desc
            .intersection(&other_desc)
            .into_iter()
            .map(|a| **a)
            .collect();
    }

    pub fn guess_type(&mut self) -> (&'static str, String) {
        let matched_types: Vec<&str> = self.descriptions.iter().map(|(t, _)| *t).collect();

        if matched_types.contains(&"boolean") {
            return ("boolean", "boolean".to_owned());
        }

        if matched_types.contains(&"integer") {
            return ("integer", "integer".to_owned());
        }

        if matched_types.contains(&"number") {
            return ("number", "number".to_owned());
        }

        if matched_types.len() == 1 && matched_types.contains(&"datetime_tz") {
            return ("datetime", self.descriptions[0].1.to_owned());
        }

        if matched_types.len() == 1 && matched_types.contains(&"datetime") {
            return ("datetime", self.descriptions[0].1.to_owned());
        }

        if matched_types.len() == 1 && matched_types.contains(&"date") {
            return ("date", self.descriptions[0].1.to_owned());
        }

        if matched_types.len() == 1 && matched_types.contains(&"time") {
            return ("time", self.descriptions[0].1.to_owned());
        }

        if matched_types.len() == 1 && matched_types.contains(&"object") {
            return ("object", "object".to_owned());
        }

        if matched_types.len() == 1 && matched_types.contains(&"array") {
            return ("array", "array".to_owned());
        }

        ("string", "string".to_owned())
    }

    pub fn stats(&mut self) -> serde_json::Value {
        if !self.options.stats && !self.options.mergable_stats {
            return serde_json::json!({});
        }

        let top_20 = self.string_freq.k_most_common_ordered(20);

        let mut deciles = vec![];
        for i in 1..10 {
            let i = f64::from(i) * 0.1;
            deciles.push(self.tdigest.quantile(i));
        }
        let deciles = if self.tdigest.is_empty() {
            None
        } else {
            Some(deciles)
        };

        let mut centiles = vec![];
        for i in 1..100 {
            let i = f64::from(i) * 0.01;
            centiles.push(self.tdigest.quantile(i));
        }
        let centiles = if self.tdigest.is_empty() {
            None
        } else {
            Some(centiles)
        };

        let empty = vec![];
        let min_string =
            String::from_utf8_lossy(self.minmax_str.min().unwrap_or(&empty)).to_string();
        let max_string =
            String::from_utf8_lossy(self.minmax_str.max().unwrap_or(&empty)).to_string();

        let is_number = ["number", "integer"].contains(&self.guess_type().0);

        if self.options.mergable_stats {
            json!({
                "min_len": self.min_len,
                "max_len": self.max_len,
                "min_str": if min_string.is_empty() {None} else {Some(min_string)},
                "max_str": if max_string.is_empty() {None} else {Some(max_string)},
                "min_number": self.min_number,
                "max_number": self.max_number,
                "count": self.count,
                "empty_count": self.empty_count,
                "estimate_unique": if self.string_freq.len() == 0 {Some(self.loglog.count())} else {None},
                "sum": if !is_number {None} else {Some(self.sum)},
                "mean": if !is_number {None} else {Some(self.sum / (self.count as f64))},
            })
        } else {
            json!({
                "min_len": self.min_len,
                "max_len": self.max_len,
                "min_str": if min_string.is_empty() {None} else {Some(min_string)},
                "max_str": if max_string.is_empty() {None} else {Some(max_string)},
                "count": self.count,
                "empty_count": self.empty_count,
                "exact_unique": if self.string_freq.len() == 0 {None} else {Some(self.string_freq.len())},
                "estimate_unique": if self.string_freq.len() == 0 {Some(self.loglog.count())} else {None},
                "top_20": if top_20.is_empty() {None} else {Some(top_20)},
                "sum": if !is_number {None} else {Some(self.stats.mean() * self.stats.len().to_f64().unwrap_or(0_f64))},
                "mean": if !is_number {None} else {Some(self.stats.mean())},
                "variance": if !is_number {None} else {Some(self.stats.variance())},
                "stddev": if !is_number {None} else {Some(self.stats.stddev())},
                "min_number": if self.tdigest.is_empty() {None} else {Some(self.tdigest.min())},
                "max_number": if self.tdigest.is_empty() {None} else {Some(self.tdigest.max())},
                "median": if self.tdigest.is_empty() {None} else {Some(self.tdigest.quantile(0.5))},
                "lower_quartile": if self.tdigest.is_empty() {None} else {Some(self.tdigest.quantile(0.25))},
                "upper_quartile": if self.tdigest.is_empty() {None} else {Some(self.tdigest.quantile(0.75))},
                "deciles": deciles,
                "centiles": centiles,
            })
        }
    }

    fn num_stats(&mut self, number: f64) {
        if self.options.stats && !number.is_nan() {
            if !self.options.mergable_stats {
                self.tdigest.insert(number);
            }
            self.stats.add(number);
            if self.max_number.is_none() {
                self.max_number = Some(number);
                self.min_number = Some(number);
            }
            self.max_number = Some(number.max(self.max_number.expect("number already checked")));
            self.min_number = Some(number.min(self.min_number.expect("number already checked")));
            self.sum += number;
        }
    }

    pub fn process_num(&mut self, number: f64) {
        if !self.descriptions.contains(&("integer", "integer"))
            || !self.descriptions.contains(&("number", "number"))
        {
            self.process(&number.to_string());
            return;
        }
        self.descriptions.clear();
        self.descriptions.push(("number", "number"));

        self.count += 1;
        self.num_stats(number);
    }

    pub fn process(&mut self, string: &str) {
        if string.is_empty() {
            self.empty_count += 1;
            return;
        }
        self.count += 1;

        if self.options.stats {
            if self.max_len.is_none() {
                self.max_len = Some(string.len());
                self.min_len = Some(string.len());
            }

            self.max_len = Some(max(self.max_len.expect("checked_for_none"), string.len()));
            self.min_len = Some(min(self.min_len.expect("checked_for_none"), string.len()));

            self.minmax_str.add(string.as_bytes().to_vec());

            self.loglog.add(string);

            if !self.no_string_stats {
                if string.len() > 100 {
                    self.no_string_stats = true;
                    self.string_freq.clear();
                } else if !self.unique_to_large {
                    if !self.options.mergable_stats {
                        self.string_freq.update([string.into()]);
                    }
                    if self.string_freq.len() > 200 {
                        self.unique_to_large = true;
                        self.no_string_stats = true;
                        self.string_freq.clear();
                    }
                }
            }
        }

        for num in 0usize..self.descriptions.len() {
            let (type_name, type_description) = self.descriptions[num];

            if type_name == "boolean" && !self.check_boolean(string) {
                self.to_delete.push(num)
            }

            if type_name == "integer" && !self.check_integer(string) {
                self.to_delete.push(num)
            }

            if type_name == "number" {
                if let Some(number) = self.check_number(string) {
                    self.num_stats(number);
                } else {
                    self.to_delete.push(num);
                    self.tdigest.clear();
                }
            }

            if ["datetime", "datetime_tz", "date", "time"].contains(&type_name) {
                if !string.is_ascii() {
                    self.to_delete.push(num);
                    continue;
                }
            }

            if type_name == "datetime" && !self.check_datetime(string, type_description) {
                self.to_delete.push(num)
            }

            if type_name == "datetime_tz" && !self.check_datetime_tz(string, type_description) {
                self.to_delete.push(num)
            }

            if type_name == "date" && !self.check_date(string, type_description) {
                self.to_delete.push(num)
            }

            if type_name == "time" && !self.check_time(string, type_description) {
                self.to_delete.push(num)
            }

            if type_name == "object" && !self.check_json_object(string) {
                self.to_delete.push(num)
            }

            if type_name == "array" && !self.check_json_array(string) {
                self.to_delete.push(num)
            }
        }

        for num in self.to_delete.iter().rev() {
            self.descriptions.remove(*num);
        }
        self.to_delete.clear()
    }

    fn check_integer(&mut self, string: &str) -> bool {
        if string.len() > 1 && string.starts_with("0") {
            return false
        }
        string.parse::<i64>().is_ok()
    }

    fn check_number(&mut self, string: &str) -> Option<f64> {
        if string.len() > 1 && string.starts_with("0") && !string.starts_with("0.") {
            return None
        }
        // floats this large loose precision
        if string.len() > 17 {
            return None
        }
        string.parse().ok()
    }

    fn check_boolean(&mut self, string: &str) -> bool {
        if ["true", "false", "t", "f", "True", "False", "TRUE", "FALSE"].contains(&string) {
            return true;
        }
        false
    }

    fn check_datetime_tz(&mut self, string: &str, format: &str) -> bool {
        if format == "rfc2822" {
            DateTime::parse_from_rfc2822(string).is_ok()
        } else if format == "rfc3339" {
            DateTime::parse_from_rfc3339(string).is_ok()
        } else if DateTime::parse_from_str(string, format).is_ok() {
            true
        } else {
            false
        }
    }

    fn check_datetime(&mut self, string: &str, format: &str) -> bool {
        NaiveDateTime::parse_from_str(string, format).is_ok()
    }

    fn check_date(&mut self, string: &str, format: &str) -> bool {
        chrono::NaiveDate::parse_from_str(string, format).is_ok()
    }

    fn check_time(&mut self, string: &str, format: &str) -> bool {
        chrono::NaiveTime::parse_from_str(string, format).is_ok()
    }

    fn check_json_array(&mut self, string: &str) -> bool {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(string) {
            if value.is_array() {
                return true;
            }
            false
        } else {
            false
        }
    }

    fn check_json_object(&mut self, string: &str) -> bool {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(string) {
            if value.is_object() {
                return true;
            }
            false
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*; //, datetime_formats, datetime_tz_formats, date_formats};
                  //use chrono::prelude::*;

    #[test]
    fn guess_bool() {
        let mut describer = Describer::new();
        describer.process("true");
        assert!(describer.guess_type().0 == "boolean");
        describer.process("truee");
        assert_eq!(describer.guess_type().0, "string");
    }
    #[test]
    fn guess_int() {
        let mut describer = Describer::new();
        describer.process("2");
        assert!(describer.guess_type().0 == "integer");
        describer.process("12132323");
        assert_eq!(describer.guess_type().0, "integer");
        describer.process("1.2");
        assert_eq!(describer.guess_type().0, "number");
        describer.process("1.2.1");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn guess_int_zero() {
        let mut describer = Describer::new();
        describer.process("2");
        assert!(describer.guess_type().0 == "integer");
        describer.process("001");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn guess_number() {
        let mut describer = Describer::new();
        describer.process("1.2");
        assert!(describer.guess_type().0 == "number");
        describer.process("1.323231877979731");
        assert!(describer.guess_type().0 == "number");
        describer.process("1.2132323");
        assert_eq!(describer.guess_type().0, "number");
        describer.process("0.32131322");
        assert_eq!(describer.guess_type().0, "number");
        describer.process("nan");
        assert_eq!(describer.guess_type().0, "number");
        describer.process("1.3232e4");
        assert_eq!(describer.guess_type().0, "number");
        describer.process("1.3232a4");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn guess_number_zero_start() {
        let mut describer = Describer::new();
        describer.process("1.2");
        assert!(describer.guess_type().0 == "number");
        describer.process("0");
        assert!(describer.guess_type().0 == "number");
        describer.process("0.1");
        assert!(describer.guess_type().0 == "number");
        describer.process("01.1");
        assert!(describer.guess_type().0 == "string");
    }

    #[test]
    fn guess_number_double_zero_start() {
        let mut describer = Describer::new();
        describer.process("1.2");
        assert!(describer.guess_type().0 == "number");
        describer.process("00.1");
        assert!(describer.guess_type().0 == "string");
    }

    #[test]
    fn guess_rfc2822() {
        let mut describer = Describer::new();
        describer.process("Fri, 28 Nov 2014 21:00:09 +0900");
        assert_eq!(describer.guess_type(), ("datetime", "rfc2822".to_owned()));
        describer.process("Fri, 32 Nov 2014 21:00:09 +0900");
        assert_eq!(describer.guess_type().0, "string");

        let mut describer = Describer::new();
        describer.process("Fri, 28 Nov 2014 21:00:09 +0900");
        assert_eq!(describer.guess_type(), ("datetime", "rfc2822".to_owned()));
        describer.process("2014-11-28T21:00:09+09:00");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn guess_rfc3339() {
        let mut describer = Describer::new();
        describer.process("2014-11-28T21:00:09+09:00");
        assert_eq!(describer.guess_type(), ("datetime", "rfc3339".to_owned()));
        describer.process("2014-13-28T21:00:09+09:00");
        assert_eq!(describer.guess_type().0, "string");

        let mut describer = Describer::new();
        describer.process("2014-11-28T21:00:09+09:00");
        assert_eq!(describer.guess_type(), ("datetime", "rfc3339".to_owned()));
        describer.process("Fri, 28 Nov 2014 21:00:09 +0900");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn guess_datetime() {
        let mut describer = Describer::new();
        describer.process("2014-11-28 21:00:09+09:00");
        assert_eq!(describer.guess_type().0, "datetime");
        describer.process("2014-13-28 21:00:09+09:00");
        assert_eq!(describer.guess_type().0, "string");

        let mut describer = Describer::new();
        describer.process("28/01/2008 21:00");
        assert_eq!(describer.guess_type().0, "datetime");
        describer.process("01/28/2008 21:00");
        assert_eq!(describer.guess_type().0, "string");

        let mut describer = Describer::new();
        describer.process("01/28/2008 21:00");
        assert_eq!(describer.guess_type().0, "datetime");
        describer.process("28/01/2008 21:00");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn guess_date() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        describer.process("2014-11-28");
        assert_eq!(describer.guess_type().0, "date");
        describer.process("2014-13-28");
        assert_eq!(describer.guess_type().0, "string");

        let mut describer = Describer::new();
        describer.process("20/11/2001");
        assert_eq!(describer.guess_type().0, "date");
        describer.process("11/20/2001");
        assert_eq!(describer.guess_type().0, "string");

        let mut describer = Describer::new();
        describer.process("11/20/2001");
        assert_eq!(describer.guess_type().0, "date");
        describer.process("20/11/2001");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn guess_time() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        describer.process("12:30");
        assert_eq!(describer.guess_type().0, "time");
        describer.process("25:00");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn json_array() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        describer.process("[]");
        describer.process("[1,2,3]");
        assert_eq!(describer.guess_type().0, "array");
        describer.process("{}");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn json_object() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        describer.process("{}");
        describer.process("{\"a\": \"b\"}");
        assert_eq!(describer.guess_type().0, "object");
        describer.process("[]");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn empty_count() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        describer.process("");
        describer.process("");
        describer.process("");
        describer.process("moo");
        assert_eq!(describer.guess_type().0, "string");
        assert_eq!(describer.empty_count, 3);
    }

    #[test]
    fn stats_string() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        describer.process("a");
        describer.process("b");
        describer.process("c");
        describer.process("c");
        insta::assert_debug_snapshot!(describer.stats());
    }

    #[test]
    fn stats_string_too_many_unique() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        for i in 0..1001 {
            describer.process(&format!("num-{i}"))
        }
        assert_eq!(describer.loglog.count(), 1012);
        insta::assert_debug_snapshot!(describer.stats());
    }

    #[test]
    fn stats_string_too_long() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());
        let mut long = String::new();
        for _ in 0..101 {
            long.push('a');
        }
        describer.process("a");
        describer.process(&long);
        insta::assert_debug_snapshot!(describer.stats());
    }

    #[test]
    fn stats_number() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());

        for num in 0..1001 {
            describer.process(&num.to_string())
        }

        insta::assert_debug_snapshot!(describer.stats());

        describer.process("a");

        insta::assert_debug_snapshot!(describer.stats());
    }

    #[test]
    fn stats_process_number() {
        let mut describer = Describer::new_with_options(Options::builder().stats(true).build());

        for num in 0..1001 {
            describer.process_num(num.to_f64().unwrap())
        }

        insta::assert_debug_snapshot!(describer.stats());

        describer.process("a");

        insta::assert_debug_snapshot!(describer.stats());
    }

    // #[test]
    // fn formats() {
    //     let utc: DateTime<Utc> = Utc::now();

    //     for format in datetime_formats() {
    //         println!("insert into test_dates values ('{}','{}','{}');", "datetime", format, utc.format(format));
    //     }
    //     for format in datetime_tz_formats() {
    //         println!("insert into test_dates values ('{}','{}','{}');", "datetime_tz", format, utc.format(format));
    //     }

    //     println!("insert into test_dates values ('{}','{}','{}');", "datetime_tz", "rfc2822", utc.to_rfc2822());
    //     println!("insert into test_dates values ('{}','{}','{}');", "datetime_tz", "rfc3339", utc.to_rfc3339());

    //     for format in date_formats() {
    //         println!("insert into test_dates values ('{}','{}','{}');", "date", format, utc.format(format));
    //     }

    //     assert!(false);

    // }
}
