use std::io::{Read, Write};

const BUFFER_SIZE: usize = 100;

pub struct Partition {
    buffer: String,
    fileoffset: usize,
    filename: String,
}

impl Partition {
    fn new(filename: String) -> Partition {
        Partition {
            buffer: String::new(),
            fileoffset: 0,
            filename,
        }
    }

    fn append(&mut self, message: &String) {
        self.buffer.push_str(message);

        if self.buffer.len() >= BUFFER_SIZE {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.filename)
                .unwrap();

            file.write_all(self.buffer[..BUFFER_SIZE / 2].as_bytes());
            self.fileoffset += self.buffer[..BUFFER_SIZE / 2].len();
            self.buffer = self.buffer[BUFFER_SIZE / 2..].to_string();
        }
    }

    fn read(&mut self, offset: usize) -> String {
        let res = if offset <= self.fileoffset {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .open(&self.filename)
                .unwrap();
            let mut buffer = String::new();
            file.read_to_string(&mut buffer);
            buffer[offset..].to_string()
        } else {
            String::new()
        };

        res + &self.buffer
    }

    fn get_offset(&self) -> usize {
        self.fileoffset + self.buffer.len()
    }
}
