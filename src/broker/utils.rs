use std::io::Write;

use tokio::{fs::OpenOptions, io::AsyncReadExt};

pub fn write_to_log_file(filename: &String, message: &String) -> Result<(), std::io::Error> {
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(filename)?;

    file.write_all(message.as_bytes())?;

    Ok(())
}

pub async fn read_from_file(filename: &String, offset: usize) -> String {
    let mut file = OpenOptions::new()
        .read(true)
        .open(filename)
        .await
        .expect("Failed to open file");

    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .await
        .expect("Failed to read from file");

    contents[offset..].to_string()
}
