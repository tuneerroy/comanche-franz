use std::io::{Read, Write};

const BUFFER_SIZE: usize = 100;

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
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.filename)
                .unwrap();

            file.write_all(self.buffer[..BUFFER_SIZE / 2].as_bytes())
                .unwrap();
            self.fileoffset += self.buffer[..BUFFER_SIZE / 2].len();
            self.buffer = self.buffer[BUFFER_SIZE / 2..].to_string();
        }
    }

    pub fn read(&mut self, offset: usize) -> String {
        let res = if offset < self.fileoffset {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .open(&self.filename)
                .unwrap();
            let mut buffer = String::new();
            file.read_to_string(&mut buffer).unwrap();
            buffer[offset + 1..].to_string()
        } else {
            String::new()
        };

        res + &self.buffer
    }

    // TODO: technically, we need to call this by a consumer
    // and keep track of the offset for each consumer group for each partition/topic
    #[allow(dead_code)]
    pub fn get_offset(&self) -> usize {
        self.fileoffset + self.buffer.len()
    }
}
