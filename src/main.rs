use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::sync::mpsc::{channel, Sender};
use std::thread;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct Measurement {
    city: String,
    min: i32,
    max: i32,
    count: i32,
    sum: i32,
}

impl Measurement {
    fn add_measurement(&mut self, measurement: Measurement) {
        if measurement.min < self.min {
            self.min = measurement.min;
        }
        if measurement.max > self.max {
            self.max = measurement.max;
        }
        self.sum = self.sum + measurement.sum;
        self.count = self.count + measurement.count;
    }

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
    Init,
    City,
    Temp(String),
    End
}

fn main() {

    let (tx, rx) = channel::<HashMap<String, Measurement>>();
    let filesize = {
        let measurements_file = File::open("measurements.txt").expect("file not found");
        measurements_file.metadata().expect("failed to stat file").len()
    };
    let thread_count = thread::available_parallelism().expect("failed to fetch cores").get() as u64 * 2;
    let chunk_size = filesize/thread_count;
    let excess = filesize% thread_count;
    for i in 0..thread_count {
        let tx = tx.clone();
        let start = chunk_size * i;
        let size = chunk_size + if i+1 == thread_count { excess } else { 0 };
        thread::spawn(move|| {
            process_chunk(start, size as usize, tx);
        });
    }
    let mut measures: HashMap<String, Measurement> = HashMap::new();
    for _ in 0..thread_count {
        let segment = rx.recv().unwrap();
        for i in segment {
            let m = measures.get_mut(&i.0);
            match m {
                None => {
                    measures.insert(i.0, i.1);
                }
                Some(measure) => {
                    measure.add_measurement(i.1);
                }
            }
        }
    }
    let mut vals = measures.into_values().collect::<Vec<Measurement>>();
    vals.sort_by(|a,b| a.city.cmp(&b.city));
    for m in vals {
        println!("{}", m);
    }
}

fn process_chunk(start: u64, size: usize, chan: Sender<HashMap<String, Measurement>>) {
    let measurements_file = File::open("measurements.txt").expect("file not found");
    let mut reader = BufReader::with_capacity(4*1024, measurements_file);
    reader.seek(SeekFrom::Start(if start > 0 { start - 1 } else { start })).expect("Failed to seek to file");
    let mut buf = vec![0; size + 50];
    let test = reader.read(&mut buf);
    if test.is_err() || test.unwrap() == 0 {
        panic!("failed to read file");
    }

    let mut string_buf: Vec<u8> = vec![];
    let mut measures: HashMap<String, Measurement> = HashMap::new();
    // let mut measures: Vec<Measurement> = vec![];
    let mut current_state: State = if start == 0 { State::City } else { State::Init };
    let mut read_bytes = 0;
    let mut last_read = false;

    let mut pos: i32 = -1;

    loop {
        pos = pos + 1;
        if pos > buf.len() as i32 {
            break;
        }
        let test_byte = buf[pos as usize];
        read_bytes = read_bytes + 1;
        if read_bytes >= size {
            last_read = true;
        }

        match current_state {
            State::Init => {
                if test_byte == 10 {
                    current_state = State::City;
                }
                continue;
            }
            State::City => {
                if test_byte == 59 {
                    let city_str = String::from_utf8(string_buf.clone()).unwrap();
                    if !measures.contains_key(&city_str) {
                        measures
                            .insert(city_str.clone(), Measurement::new(city_str.clone()));
                    }
                    string_buf.clear();
                    current_state = State::Temp(city_str);
                    continue;
                }

                string_buf.push(test_byte);
            }
            State::Temp(ref city_str) => {
                // check for space
                if test_byte == 32 || test_byte == 46 {
                    continue;
                }

                if test_byte == 10 {
                    let current_measure = measures.get_mut(city_str).unwrap();
                    current_measure.take_measure(String::from_utf8(string_buf.clone())
                        .unwrap()
                        .parse()
                        .unwrap());
                    current_state = if last_read { State::End } else { State::City };
                    string_buf.clear();
                    continue;
                }

                string_buf.push(test_byte);
            }
            State::End => break
        }
    }

    chan.send(measures).expect("failed to send measurements");
}