use std::io::{Read, Write};

const BUFFER_SIZE: usize = 100;
const FILE_PATH: &str = "data/";

pub struct Partition {
    buffer: String,
    fileoffset: usize,
    filename: String,
}

impl Partition {
    pub fn new(filename: String) -> Partition {
        Partition {
            buffer: String::new(),
            fileoffset: 0,
            filename,
        }
    }

    pub fn append(&mut self, message: &String) {
        self.buffer.push_str(message);

        if self.buffer.len() >= BUFFER_SIZE {
            let filename = FILE_PATH.to_string() + &self.filename;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(filename)
                .unwrap();

            file.write_all(self.buffer[..BUFFER_SIZE / 2].as_bytes())
                .unwrap();
            self.fileoffset += self.buffer[..BUFFER_SIZE / 2].len();
            self.buffer = self.buffer[BUFFER_SIZE / 2..].to_string();
        }
    }

    pub fn read(&mut self, offset: usize) -> String {
        if offset < self.fileoffset {
            let filename = FILE_PATH.to_string() + &self.filename;
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .open(filename)
                .unwrap();
            let mut buffer = String::new();
            file.read_to_string(&mut buffer).unwrap();
            buffer[offset..].to_string() + &self.buffer
        } else {
            self.buffer[offset - self.fileoffset..].to_string()
        }
    }

    pub fn get_offset(&self) -> usize {
        self.fileoffset + self.buffer.len()
    }
}
