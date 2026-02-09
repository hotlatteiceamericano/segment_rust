use anyhow::Context;

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

use crate::storable::Storable;

#[derive(Debug)]
pub struct Segment<T> {
    base_offset: u64,
    write_position: u64,
    file: File,
    path: PathBuf,
    _marker: PhantomData<T>,
}

pub const FILE_EXTENSION: &str = "segment";

// next:
// 1. Segment to take a generic type deciding which type it should be storing
// 2. this type should be serializable for write, and deserializable for read
impl<T: Storable> Segment<T> {
    pub const LENGTH: u32 = 4;

    /// # Arguments
    /// * parent_directory: the parent directory you are placing the segment file
    ///   it will create the parent directory if not exist
    /// * base_offset: segment use the base_offset as its filename
    pub fn new(parent_directory: &Path, base_offset: u64) -> anyhow::Result<Self> {
        if !parent_directory.exists() {
            fs::create_dir_all(parent_directory)
                .context("cannot create parent directory when instantiate segment")?;
        }

        let file_path = parent_directory
            .join(format!("{:08}", base_offset))
            .with_extension(FILE_EXTENSION);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&file_path)?;

        Ok(Self {
            base_offset,
            // todo: replace 0 with current length + 1
            write_position: 0,
            file,
            path: file_path,
            _marker: PhantomData,
        })
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn write_position(&self) -> u64 {
        self.write_position
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// # Arguments
    /// * `message` - the message being written to the segment
    /// # Returns
    /// new local write offset after written the given message
    pub fn write(&mut self, record: &T) -> io::Result<u64> {
        let serialized_msg = bincode::serialize(record)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.file
            .write_all(&record.content_length().to_be_bytes())?;
        self.file.write_all(&serialized_msg)?;
        self.file.flush()?;

        self.write_position += record.total_length() as u64;

        Ok(self.write_position)
    }

    /// #Arguments
    /// * `offset` - the local offset to this file
    /// it is expected for topic to find the local offset from a global offset
    /// # Returns
    /// message at the give offset
    pub fn read(&mut self, offset: u64) -> io::Result<T> {
        self.file.seek(io::SeekFrom::Start(offset))?;

        let mut len_bytes = [0u8; Self::LENGTH as usize];
        self.file.read_exact(&mut len_bytes)?;
        let msg_len = u32::from_be_bytes(len_bytes);

        let mut msg_bytes = vec![0u8; msg_len as usize];
        self.file.read_exact(&mut msg_bytes)?;

        bincode::deserialize::<T>(&msg_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

// todo: need to test this from function
// also test if setting write_position to be length +1 is correct or not
impl<T> From<PathBuf> for Segment<T> {
    fn from(path: PathBuf) -> Self {
        let filename = path.file_name().unwrap();
        let file = fs::File::open(&path).unwrap();

        let len = file.metadata().unwrap().len();

        Self {
            base_offset: filename.to_str().unwrap().parse::<u64>().unwrap(),
            file,
            write_position: len + 1,
            path,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {

    use crate::message::Message;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;

    use rand::Rng;
    use rstest::fixture;
    use rstest::rstest;

    use crate::segment::Segment;

    #[fixture]
    fn random_parent_directory() -> PathBuf {
        let mut rng = rand::thread_rng();
        let random_parent_dir: String = (0..8)
            .map(|_| {
                let idx = rng.gen_range(0..26);
                (b'a' + idx) as char
            })
            .collect();
        std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(random_parent_dir)
    }

    #[rstest]
    fn test_write(random_parent_directory: PathBuf) {
        let mut segment = Segment::new(&random_parent_directory, 0).unwrap();
        let message = Message::new("hello world!");

        let latest_offset = segment.write(&message).unwrap();

        let serialized_msg = bincode::serialize(&message.content);
        assert_eq!(latest_offset, 4 + serialized_msg.unwrap().len() as u64);

        remove_path(&random_parent_directory);
    }

    #[rstest]
    pub fn test_read(random_parent_directory: PathBuf) {
        let mut segment = Segment::new(&random_parent_directory, 0).unwrap();

        let message = Message::new("hello world!");
        segment.write(&message).unwrap();

        let message_read = segment
            .read(0)
            .unwrap_or_else(|e| panic!("error when read from the segment: {:#?}", e));
        assert_eq!(message_read.content, "hello world!");

        remove_path(&random_parent_directory);
    }

    fn remove_path(path: &Path) {
        fs::remove_dir_all(path).unwrap();
    }
}
