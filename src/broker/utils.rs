use std::{io::{Read, Write}, collections::HashMap};

use crate::{ConsumerGroupId, consumer_group};

const BUFFER_SIZE: usize = 100;
const FILE_PATH: &str = "data/";

pub struct Partition {
    buffer: String,
    fileoffset: usize,
    filename: String,
    consumer_group_id_to_offset: HashMap<ConsumerGroupId, usize>,
}

impl Partition {
    pub fn new(filename: String) -> Partition {
        Partition {
            buffer: String::new(),
            fileoffset: 0,
            filename,
            consumer_group_id_to_offset: HashMap::new(),
        }
    }

    pub fn initialize_offset(&mut self, consumer_group_id: ConsumerGroupId) {
        let offset = self.fileoffset + self.buffer.len(); 
        self.consumer_group_id_to_offset.insert(consumer_group_id, offset);
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

    pub fn read(&mut self, consumer_group_id: ConsumerGroupId) -> String {
        let default_offset = self.fileoffset + self.buffer.len();
        let offset = self.consumer_group_id_to_offset
            .entry(consumer_group_id)
            .or_insert(default_offset);

        let res;
        if *offset < self.fileoffset {
            let filename = FILE_PATH.to_string() + &self.filename;
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .open(filename)
                .unwrap();
            let mut buffer = String::new();
            file.read_to_string(&mut buffer).unwrap();
            res = buffer[*offset..].to_string() + &self.buffer;
        } else {
            res = self.buffer[*offset - self.fileoffset..].to_string();
        }
        *offset = self.fileoffset + self.buffer.len();
        res
    }
}
