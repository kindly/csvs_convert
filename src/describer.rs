use chrono::prelude::*;
use chrono::DateTime;

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

fn datetime_formats() ->  Vec<&'static str> {
    vec! [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %I:%M:%S %P",
        "%Y-%m-%d %I:%M %P",
        "%Y %b %d %H:%M:%S",
        "%B %d %Y %H:%M:%S",
        "%B %d %Y %I:%M:%S %P",
        "%B %d %Y %I:%M %P",
        "%Y %b %d at %I:%M %P",
        "%d %B %Y %H:%M:%S",
        "%d %B %Y %H:%M",
        "%d %B %Y %H:%M:%S.%f",
        "%d %B %Y %I:%M:%S %P",
        "%d %B %Y %I:%M %P",
        "%B %d %Y %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%y %H:%M:%S.%f",
        "%m/%d/%y %I:%M:%S %P",
        "%m/%d/%y %I:%M %P",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S.%f",
        "%m/%d/%Y %I:%M:%S %P",
        "%m/%d/%Y %I:%M %P",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d/%m/%y %H:%M:%S.%f",
        "%d/%m/%y %I:%M:%S %P",
        "%d/%m/%y %I:%M %P",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S.%f",
        "%d/%m/%Y %I:%M:%S %P",
        "%d/%m/%Y %I:%M %P",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S.%f",
        "%Y/%m/%d %I:%M:%S %P",
        "%Y/%m/%d %I:%M %P",
        "%y%m%d %H:%M:%S",
    ]
}

fn datetime_tz_formats() ->  Vec<&'static str> {
    vec! [
        "%Y-%m-%d %H:%M:%S%#z",
        "%Y-%m-%d %H:%M:%S.%f%#z",
        "%Y-%m-%d %H:%M%#z",
        "%Y-%m-%d %H:%M:%S %Z",
        "%Y-%m-%d %H:%M:%S.%f %Z",
        "%B %d %Y %H:%M:%S %Z",
        "%B %d %Y %H:%M %Z",
        "%B %d %Y %I:%M:%S %P %Z",
        "%B %d %Y %I:%M %P %Z",
    ]
}

fn date_formats() -> Vec<&'static str> {
    vec! [
        "%Y-%m-%d",
        "%Y-%b-%d",
        "%B %d %y",
        "%B %d %Y",
        "%d %B %y",
        "%d %B %Y",
        "%m/%d/%y",
        "%m/%d/%Y",
        "%d/%m/%y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%m.%d.%Y",
        "%Y.%m.%d",
    ]
}

fn time_formats() -> Vec<&'static str> {
    vec! [
        "%H:%M",
        "%I:%M:%S %P",
        "%I:%M %P",
    ]
}

#[derive(Debug)]
pub struct Describer {
    count: usize,
    empty_count: usize,
    descriptions: Vec<(&'static str, &'static str)>,
    to_delete: Vec<usize> 
}

impl Describer {
    pub fn new() -> Describer {
        return Describer {
            count: 0,
            empty_count: 0,
            descriptions: descriptions(),
            to_delete: vec![]
        }
    }

    pub fn guess_type(&mut self) -> (&'static str, String) {
        let matched_types: Vec<&str> = self.descriptions.iter().map(|(t, _)| *t).collect();

        if matched_types.contains(&"boolean") {
            return ("boolean", "boolean".to_owned())
        }

        if matched_types.contains(&"integer") {
            return ("integer", "integer".to_owned())
        }

        if matched_types.contains(&"number") {
            return ("number", "number".to_owned())
        }

        if matched_types.len() == 1 && matched_types.contains(&"datetime_tz") {
            return ("datetime", self.descriptions[0].1.to_owned())
        }

        if matched_types.len() == 1 && matched_types.contains(&"datetime") {
            return ("datetime", self.descriptions[0].1.to_owned())
        }

        if matched_types.len() == 1 && matched_types.contains(&"date") {
            return ("date", self.descriptions[0].1.to_owned())
        }

        if matched_types.len() == 1 && matched_types.contains(&"time") {
            return ("time", self.descriptions[0].1.to_owned())
        }

        if matched_types.len() == 1 && matched_types.contains(&"object") {
            return ("object", "object".to_owned())
        }

        if matched_types.len() == 1 && matched_types.contains(&"array") {
            return ("array", "array".to_owned())
        }

        return ("string", "string".to_owned())
    }

    pub fn process(&mut self, string: &str){
        self.count += 1;

        if string.is_empty() {
            self.empty_count += 1;
            return
        }

        for num in 0usize..self.descriptions.len() {
            let (type_name, type_description) = self.descriptions[num];

            if type_name == "boolean" {
                if !self.check_boolean(string) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "integer" {
                if !self.check_integer(string) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "number" {
                if !self.check_number(string) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "datetime" {
                if !self.check_datetime(string, type_description) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "datetime_tz" {
                if !self.check_datetime_tz(string, type_description) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "date" {
                if !self.check_date(string, type_description) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "time" {
                if !self.check_time(string, type_description) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "object" {
                if !self.check_json_object(string) {
                    self.to_delete.push(num)
                }
            }

            if type_name == "array" {
                if !self.check_json_array(string) {
                    self.to_delete.push(num)
                }
            }
        }

        for num in self.to_delete.iter().rev() {
            self.descriptions.remove(*num);
        }
        self.to_delete.clear()

    }

    fn check_integer(&mut self, string: &str) -> bool {
        if let Ok(_) = string.parse::<i128>() {
            true
        } else {
            false
        }
    }

    fn check_number(&mut self, string: &str) -> bool {
        if let Ok(_) = string.parse::<f64>() {
            true
        } else {
            false
        }
    }

    fn check_boolean(&mut self, string: &str) -> bool {
        if ["1", "0", "true", "false", "t", "f", "True", "False", "TRUE", "FALSE"].contains(&string) {
            return true
        }
        false
    }

    fn check_datetime_tz(&mut self, string: &str, format: &str) -> bool {
        if format == "rfc2822" {
            if let Ok(_) = DateTime::parse_from_rfc2822(string) {
                return true
            } else {
                return false
            }
        } else if format == "rfc3339" {
            if let Ok(_) = DateTime::parse_from_rfc3339(string) {
                return true
            } else {
                return false
            }
        } else {
            if let Ok(_) = DateTime::parse_from_str(string, format) {
                println!("true");
                return true
            } else {
                return false
            }
        }
    }

    fn check_datetime(&mut self, string: &str, format: &str) -> bool {
        if let Ok(_) = chrono::Utc.datetime_from_str(string, format){
            return true
        } else {
            return false
        }
    }

    fn check_date(&mut self, string: &str, format: &str) -> bool {
        if let Ok(_) = chrono::NaiveDate::parse_from_str(string, format){
            return true
        } else {
            return false
        }
    }

    fn check_time(&mut self, string: &str, format: &str) -> bool {
        if let Ok(_) = chrono::NaiveTime::parse_from_str(string, format){
            return true
        } else {
            return false
        }
    }

    fn check_json_array(&mut self, string: &str) -> bool {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(string){
            if value.is_array() {
                return true
            }
            return false
        } else {
            return false
        }
    }

    fn check_json_object(&mut self, string: &str) -> bool {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(string){
            if value.is_object() {
                return true
            }
            return false
        } else {
            return false
        }
    }

}


#[cfg(test)]
mod tests {
    use super::Describer; //, datetime_formats, datetime_tz_formats, date_formats};
    //use chrono::prelude::*;

    #[test]
    fn guess_bool() {
        let mut describer = Describer::new();
        describer.process("1");
        assert!(describer.guess_type().0 == "boolean");
        describer.process("true");
        assert!(describer.guess_type().0 == "boolean");
        describer.process("truee");
        assert_eq!(describer.guess_type().0, "string");
    }
    #[test]
    fn guess_int() {
        let mut describer = Describer::new();
        describer.process("1");
        assert!(describer.guess_type().0 == "boolean");
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
        let mut describer = Describer::new();
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
        let mut describer = Describer::new();
        describer.process("12:30");
        assert_eq!(describer.guess_type().0, "time");
        describer.process("25:00");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn json_array() {
        let mut describer = Describer::new();
        describer.process("[]");
        describer.process("[1,2,3]");
        assert_eq!(describer.guess_type().0, "array");
        describer.process("{}");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn json_object() {
        let mut describer = Describer::new();
        describer.process("{}");
        describer.process("{\"a\": \"b\"}");
        assert_eq!(describer.guess_type().0, "object");
        describer.process("[]");
        assert_eq!(describer.guess_type().0, "string");
    }

    #[test]
    fn empty_count() {
        let mut describer = Describer::new();
        describer.process("");
        describer.process("");
        describer.process("");
        describer.process("moo");
        assert_eq!(describer.guess_type().0, "string");
        assert_eq!(describer.empty_count, 3);
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