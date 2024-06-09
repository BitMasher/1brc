use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::ops::BitXor;
use std::sync::mpsc::{channel, Sender};
use std::thread;

/// 64-bit hash constant from FxHash
const FX_HASH_CONST: usize = 0x517cc1b727220a95;
type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

/// Pulled out of the rustc_hash crate to avoid having it as a dependency
#[derive(Default, Clone)]
struct FxHasher {
    hash: usize,
}

impl FxHasher {
    #[inline]
    fn add_to_hash(&mut self, i: usize) {
        self.hash = self
            .hash
            .rotate_left(5)
            .bitxor(i)
            .wrapping_mul(FX_HASH_CONST);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash as u64
    }

    #[inline]
    fn write(&mut self, mut bytes: &[u8]) {
        const _: () = assert!(size_of::<usize>() <= size_of::<u64>());
        const _: () = assert!(size_of::<u32>() <= size_of::<usize>());
        let mut state = self.clone();
        while let Some(&usize_bytes) = take_first_chunk(&mut bytes) {
            state.add_to_hash(usize::from_ne_bytes(usize_bytes));
        }
        if let Some(&u32_bytes) = take_first_chunk(&mut bytes) {
            state.add_to_hash(u32::from_ne_bytes(u32_bytes) as usize);
        }
        if let Some(&u16_bytes) = take_first_chunk(&mut bytes) {
            state.add_to_hash(u16::from_ne_bytes(u16_bytes) as usize);
        }
        if let Some(&[u8_byte]) = take_first_chunk(&mut bytes) {
            state.add_to_hash(u8_byte as usize);
        }
        *self = state;
    }
}

#[inline]
fn take_first_chunk<'a, const N: usize>(slice: &mut &'a [u8]) -> Option<&'a [u8; N]> {
    let (first, rest) = slice.split_first_chunk()?;
    *slice = rest;
    Some(first)
}

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
    City(usize),
    Temp(usize, usize),
    End,
}

fn main() {
    let (tx, rx) = channel::<HashMap<Vec<u8>, Measurement, BuildHasherDefault<FxHasher>>>();
    let filesize = {
        let measurements_file = File::open("measurements.txt").expect("file not found");
        measurements_file
            .metadata()
            .expect("failed to stat file")
            .len()
    };
    let thread_count = thread::available_parallelism()
        .expect("failed to fetch cores")
        .get() as u64
        * 2;
    let chunk_size = filesize / thread_count;
    let excess = filesize % thread_count;
    for i in 0..thread_count {
        let tx = tx.clone();
        let start = chunk_size * i;
        let size = chunk_size + if i + 1 == thread_count { excess } else { 0 };
        thread::spawn(move || {
            process_chunk(start, size as usize, tx);
        });
    }
    let mut measures: HashMap<Vec<u8>, Measurement, BuildHasherDefault<FxHasher>> = FxHashMap::with_hasher(Default::default());
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
    vals.sort_by(|a, b| a.city.cmp(&b.city));
    for m in vals {
        println!("{}", m);
    }
}

fn process_chunk(start: u64, size: usize, chan: Sender<HashMap<Vec<u8>, Measurement, BuildHasherDefault<FxHasher>>>) {
    let mut measurements_file = File::open("measurements.txt").expect("file not found");
    measurements_file
        .seek(SeekFrom::Start(if start > 0 { start - 1 } else { start }))
        .expect("Failed to seek to file");
    let mut buf = vec![0; size + 50];
    let test = measurements_file.read(&mut buf);
    if test.is_err() || test.unwrap() == 0 {
        panic!("failed to read file");
    }

    let mut string_buf: Vec<u8> = vec![];
    let mut measures: HashMap<Vec<u8>, Measurement, BuildHasherDefault<FxHasher>> = FxHashMap::with_hasher(Default::default());
    // let mut measures: Vec<Measurement> = vec![];
    let mut current_state: State = if start == 0 {
        State::City(0)
    } else {
        State::Init
    };
    let mut read_bytes = 0;
    let mut last_read = false;

    for pos in 0..buf.len() {
        let test_byte = buf[pos];
        read_bytes = read_bytes + 1;
        if read_bytes >= size {
            last_read = true;
        }

        match current_state {
            State::Init => {
                if test_byte == 10 {
                    current_state = State::City(pos + 1);
                }
                continue;
            }
            State::City(c_start) => {
                if test_byte == 59 {
                    let city_str = &buf[c_start..pos];
                    if !measures.contains_key(&city_str.to_vec()) {
                        measures.insert(city_str.to_vec(), Measurement::new(String::from_utf8(city_str.to_vec()).expect("invalid string")));
                    }
                    current_state = State::Temp(c_start, pos);
                    continue;
                }
            }
            State::Temp(c_start, c_end) => {
                // check for space
                if test_byte == 46 {
                    continue;
                }

                if test_byte == 10 {
                    let city_str = &buf[c_start..c_end];
                    let current_measure = measures.get_mut(&city_str.to_vec()).unwrap();
                    current_measure.take_measure(
                        String::from_utf8(string_buf.clone())
                            .unwrap()
                            .parse()
                            .unwrap(),
                    );
                    current_state = if last_read {
                        State::End
                    } else {
                        State::City(pos + 1)
                    };
                    string_buf.clear();
                    continue;
                }

                string_buf.push(test_byte);
            }
            State::End => break,
        }
    }

    chan.send(measures).expect("failed to send measurements");
}
