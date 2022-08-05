use crate::{Command, KVStoreError, KvsEngine, Result};
use dashmap::DashMap;
use log::{info, warn};
use serde_json::Deserializer;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{create_dir_all, read_dir, remove_file, File, OpenOptions};
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

const MAX_USELESS_SIZE: u64 = 1024 * 1024;

/** A KvStore stores key/value pairs using BitCask.
# Example
```
use std::env;
use kvs::{KvStore, Result};
use crate::kvs::KvsEngine;
# fn try_main() -> Result<()> {

let mut store = KvStore::open(env::current_dir()?)?;

store.set("1".to_owned(),"1".to_owned())?;
assert_eq!(store.get("1".to_owned())?, Some("1".to_owned()));

store.remove("1".to_owned())?;
assert_eq!(store.get("1".to_owned())?, None);
# Ok(())
# }
```
 */
#[derive(Clone)]
pub struct KvStore {
    index: Arc<DashMap<String, CommandPosition>>,
    writer: Arc<Mutex<Writer>>,
    readers: Reader,
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = Arc::new(path.into());
        create_dir_all(dir_path.as_path())?;

        let mut index = Arc::new(DashMap::new());
        let mut readers = HashMap::new();

        let (current_file_number, useless_size) =
            Self::recover(&dir_path, &mut readers, &mut index)?;

        let current_file_path = dir_path.join(format!("data_{}.txt", current_file_number));

        let current_writer = BufWriterWithPosition::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&current_file_path)?,
        )?;

        if current_file_number == 0 {
            readers.insert(
                current_file_number,
                BufReader::new(File::open(&current_file_path)?),
            );
        }

        let readers = Reader {
            dir_path: Arc::clone(&dir_path),
            compaction_number: Arc::new(AtomicU64::new(0)),
            readers: RefCell::new(readers),
        };

        let writer = Arc::new(Mutex::new(Writer {
            current_writer,
            current_file_number,
            useless_size,
            dir_path,
            index: Arc::clone(&index),
            reader: readers.clone(),
        }));

        Ok(KvStore {
            readers,
            writer,
            index,
        })
    }

    fn recover(
        dir_path: &Arc<PathBuf>,
        current_readers: &mut HashMap<u64, BufReader<File>>,
        index: &mut Arc<DashMap<String, CommandPosition>>,
    ) -> Result<(u64, u64)> {
        let mut versions: Vec<u64> = read_dir(dir_path.as_path())?
            .flat_map(|res| res.map(|e| e.path()))
            .filter(|path| path.is_file() && path.extension() == Some("txt".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| {
                        filename
                            .trim_start_matches("data_")
                            .trim_end_matches(".txt")
                    })
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();
        versions.sort();

        let mut useless_size = 0;
        for version in &versions {
            let file_path = dir_path.join(format!("data_{}.txt", version));
            let reader = BufReader::new(File::open(&file_path)?);
            let mut iter = Deserializer::from_reader(reader).into_iter::<Command>();
            let mut before_offset = iter.byte_offset() as u64;
            while let Some(command) = iter.next() {
                let after_offset = iter.byte_offset() as u64;
                match command? {
                    Command::SET(key, _) => {
                        useless_size += index
                            .insert(
                                key,
                                CommandPosition {
                                    offset: before_offset,
                                    length: after_offset - before_offset,
                                    file_number: *version,
                                },
                            )
                            .map(|cp| cp.length)
                            .unwrap_or(0);
                    }
                    Command::RM(key) => {
                        useless_size += index.remove(&key).map(|(_, cp)| cp.length).unwrap_or(0);
                        useless_size += after_offset - before_offset;
                    }
                };
                before_offset = after_offset;
            }
            current_readers.insert(*version, BufReader::new(File::open(&file_path)?));
        }

        Ok((*versions.last().unwrap_or(&0), useless_size))
    }
}

impl KvsEngine for KvStore {
    /// Set the value of a string key to a string. Return an error if the value is not written successfully.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Get the string value of a string key. If the key does not exist, return None. Return an error if the value is not read successfully.
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(entry) = self.index.get(&key) {
            self.readers.read_command(entry.value())
        } else {
            Ok(None)
        }
    }

    /// Remove a given key. Return an error if the key does not exist or is not removed successfully.
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

struct Reader {
    dir_path: Arc<PathBuf>,
    compaction_number: Arc<AtomicU64>,
    readers: RefCell<HashMap<u64, BufReader<File>>>,
}

impl Clone for Reader {
    fn clone(&self) -> Self {
        Reader {
            dir_path: Arc::clone(&self.dir_path),
            compaction_number: Arc::clone(&self.compaction_number),
            readers: RefCell::new(HashMap::new()),
        }
    }
}

impl Reader {
    fn try_to_remove_stale_readers(&self) {
        let compaction_number = self.compaction_number.load(Ordering::SeqCst);
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let reader_number = *readers.keys().next().unwrap();
            if compaction_number <= reader_number {
                break;
            }
            readers.remove(&reader_number);
        }
    }

    fn read_add<F, R>(&self, position: &CommandPosition, f: F) -> Result<R>
    where
        F: FnOnce(Take<&mut BufReader<File>>) -> Result<R>,
    {
        self.try_to_remove_stale_readers();

        let mut readers = self.readers.borrow_mut();

        if let Entry::Vacant(entry) = readers.entry(position.file_number) {
            let new_reader = BufReader::new(File::open(
                &self
                    .dir_path
                    .join(format!("data_{}.txt", position.file_number)),
            )?);
            entry.insert(new_reader);
        }

        let source_reader = readers
            .get_mut(&position.file_number)
            .expect("Can not find key in files but it is in memory");
        source_reader.seek(SeekFrom::Start(position.offset))?;
        let data_reader = source_reader.take(position.length as u64);
        f(data_reader)
    }

    fn read_command(&self, position: &CommandPosition) -> Result<Option<String>> {
        self.read_add(position, |data_reader| {
            if let Command::SET(_, value) = serde_json::from_reader(data_reader)? {
                Ok(Some(value))
            } else {
                Err(KVStoreError::UnknownCommandType)
            }
        })
    }

    fn copy_data_to_writer(
        &self,
        position: &CommandPosition,
        writer: &mut BufWriterWithPosition<File>,
    ) -> Result<()> {
        self.read_add(position, |mut data_reader| {
            io::copy(&mut data_reader, writer)?;
            Ok(())
        })
    }

    fn remove_useless_reader(&mut self, file_number: u64) -> Result<()> {
        let mut readers = self.readers.borrow_mut();
        let delete_file_numbers: Vec<u64> = readers
            .iter()
            .map(|(key, _)| *key)
            .filter(|key| *key < file_number)
            .collect();

        for number in delete_file_numbers {
            readers.remove(&number);
            let file_path = self.dir_path.join(format!("data_{}.txt", number));
            if let Err(err) = remove_file(&file_path) {
                warn!("can not delete file {:?} because {}", file_path, err);
            }
        }

        Ok(())
    }
}

struct Writer {
    dir_path: Arc<PathBuf>,
    reader: Reader,
    current_writer: BufWriterWithPosition<File>,
    current_file_number: u64,
    useless_size: u64,
    index: Arc<DashMap<String, CommandPosition>>,
}

impl Writer {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::SET(key, value);
        let data = serde_json::to_vec(&command)?;

        let offset = self.current_writer.get_position();
        self.current_writer.write_all(&data)?;
        self.current_writer.flush()?;
        let length = self.current_writer.get_position() - offset;
        let file_number = self.current_file_number;

        if let Command::SET(key, _) = command {
            self.useless_size += self
                .index
                .insert(
                    key,
                    CommandPosition {
                        offset,
                        length,
                        file_number,
                    },
                )
                .map(|cp| cp.length)
                .unwrap_or(0);
        }

        if self.useless_size > MAX_USELESS_SIZE {
            let now = SystemTime::now();
            info!("Compaction starts");
            self.compact()?;
            info!("Compaction finished, cost {:?}", now.elapsed());
        }

        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.get(&key).is_some() {
            self.useless_size += self
                .index
                .remove(&key)
                .map(|(_, cp)| cp.length)
                .unwrap_or(0);

            let command = serde_json::to_vec(&Command::RM(key))?;
            let offset = self.current_writer.get_position();
            self.current_writer.write_all(&command)?;
            self.current_writer.flush()?;

            self.useless_size += self.current_writer.get_position() - offset;

            if self.useless_size > MAX_USELESS_SIZE {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KVStoreError::KeyNotFound)
        }
    }

    fn compact(&mut self) -> Result<()> {
        self.create_new_file()?;

        let mut before_offset = 0;
        for mut entry in self.index.iter_mut() {
            let position = entry.value_mut();
            self.reader
                .copy_data_to_writer(position, &mut self.current_writer)?;
            let after_offset = self.current_writer.position;
            *position = CommandPosition {
                offset: before_offset,
                length: after_offset - before_offset,
                file_number: self.current_file_number,
            };
            before_offset = after_offset;
        }
        self.current_writer.flush()?;

        self.reader
            .compaction_number
            .store(self.current_file_number, Ordering::SeqCst);

        self.reader
            .remove_useless_reader(self.current_file_number)?;

        self.useless_size = 0;

        self.create_new_file()?;

        Ok(())
    }

    fn create_new_file(&mut self) -> Result<()> {
        self.current_file_number += 1;
        self.current_writer = BufWriterWithPosition::new(
            OpenOptions::new().create(true).append(true).open(
                &self
                    .dir_path
                    .join(format!("data_{}.txt", self.current_file_number)),
            )?,
        )?;
        Ok(())
    }
}

/// a struct which records writer's current position
struct BufWriterWithPosition<T: Write + Seek> {
    position: u64,
    writer: BufWriter<T>,
}

impl<T: Write + Seek> BufWriterWithPosition<T> {
    fn new(mut inner: T) -> Result<Self> {
        let position = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPosition {
            position,
            writer: BufWriter::new(inner),
        })
    }

    fn get_position(&self) -> u64 {
        self.position
    }
}

impl<T: Write + Seek> Write for BufWriterWithPosition<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.position += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// a struct which records command's metadata
struct CommandPosition {
    offset: u64,
    length: u64,
    file_number: u64,
}
