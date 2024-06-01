use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufReader, Read};

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct Measurement {
    city: String,
    min: i32,
    max: i32,
    count: i32,
    sum: i32,
}

impl Measurement {
    fn take_measure(&mut self, measure: i32) {
        if measure < self.min {
            self.min = measure.clone();
        }
        if measure > self.max {
            self.max = measure.clone();
        }
        self.sum = self.sum + measure;
        self.count = self.count + 1;
    }

    fn new(city: String) -> Self {
        Self {
            city,
            min: i32::MAX,
            max: i32::MIN,
            count: Default::default(),
            sum: Default::default(),
        }
    }
}

impl Default for Measurement {
    fn default() -> Self {
        Self {
            city: Default::default(),
            min: i32::MAX,
            max: i32::MIN,
            count: Default::default(),
            sum: Default::default(),
        }
    }
}

impl Display for Measurement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{};{:.1};{:.1};{:.1}",
            self.city,
            self.min as f32 / 10.0,
            (self.sum / self.count) as f32 / 10.0,
            self.max as f32 / 10.0
        )
    }
}

enum State {
    City,
    Temp,
}

fn main() {
    let measurements_file = File::open("measurements.txt").expect("file not found");
    let mut reader = BufReader::new(measurements_file);
    let mut string_buf: Vec<u8> = vec![];
    let mut measures: HashMap<String, Measurement> = HashMap::new();
    // let mut measures: Vec<Measurement> = vec![];
    let mut current_state: State = State::City;
    let mut current_city: String = Default::default();
    loop {
        let mut test_byte: [u8; 1] = [0; 1];
        let test = reader.read(&mut test_byte);
        if test.is_err() || test.unwrap() == 0 {
            break;
        }

        match current_state {
            State::City => {
                if test_byte[0] == 59 {
                    current_city.push_str(String::from_utf8(string_buf.clone()).expect("utf-8 err").as_str());
                    let m = measures.get_mut(&current_city.clone());
                    match m {
                        None => {
                            measures
                                .insert(current_city.clone(), Measurement::new(current_city.clone()));
                        }
                        _ => {}
                    }
                    string_buf.clear();
                    current_state = State::Temp;
                    continue;
                }

                string_buf.push(test_byte[0]);
            }
            State::Temp => {
                // check for space
                if test_byte[0] == 32 || test_byte[0] == 46 {
                    continue;
                }

                if test_byte[0] == 10 {
                    let current_measure = measures.get_mut(&current_city.clone()).expect("missing measure");
                    current_measure.take_measure(String::from_utf8(string_buf.clone())
                        .expect("utf-8 format err")
                        .parse()
                        .expect("invalid measurement format"));
                    current_state = State::City;
                    string_buf.clear();
                    current_city.clear();
                    continue;
                }

                string_buf.push(test_byte[0]);
            }
        }
    }
    let mut vals = measures.into_values().collect::<Vec<Measurement>>();
    vals.sort_by(|a,b| a.city.cmp(&b.city));
    for m in vals {
        println!("{}", m);
    }
}
