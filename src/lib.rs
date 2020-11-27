#[macro_use]
extern crate serde_derive;

extern crate byteorder;
extern crate crc;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::crc32;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::Path;

// This code will be processing lots of Vec<u8> data. Because they'll be used
// in the same way as String tends to be used, ByteString is a useful alias.
type ByteString = Vec<u8>;
// ByteStr is to &str what ByteString is to Vec<u8>.
type ByteStr = [u8];

#[derive(Debug, Serialize, Deserialize)]
pub struct KeyValuePair {
    pub key: ByteString,
    pub value: ByteString,
}

#[derive(Debug)]
pub struct ActionKV {
    f: File,
    // Maintains a mapping between keys and file locations.
    pub index: HashMap<ByteString, u64>,
}

impl ActionKV {
    pub fn open(path: &Path) -> io::Result<Self> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        Ok(ActionKV {
            f: f,
            index: HashMap::new(),
        })
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);

        loop {
            // We use the `File::seek()` method to return the number of bytes from the
            // start of the file. This becomes the value of the index.
            let current_position = f.seek(SeekFrom::Current(0))?;

            // ActionKV::process_record() reads a record in the file at its current position.
            let maybe_kv = ActionKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => match err.kind() {
                    // "Unexpected" is relative. The application may not have expect to encounter
                    // the end of the file, but we expect files to be finite and so we deal with
                    // that eventually.
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };

            self.index.insert(kv.key, current_position);
        }

        Ok(())
    }

    /// Reads a record in the file at its current position.
    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair> {
        // The byteorder crate allows on-disk integers to be read in a deterministic manner.
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let val_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + val_len;

        let mut data = ByteString::with_capacity(data_len as usize);

        {
            // `f.by_ref` is required because `take(n)` creates a new Read value.
            // Using a reference within this short-lived block allows us to sidestep
            // ownership issues.
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }
        debug_assert_eq!(data.len(), data_len as usize);

        let checksum = crc32::checksum_ieee(&data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }

        // `split_off(n)` splits a Vec<T> in two at `n`.
        let val = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair {
            key: key,
            value: val,
        })
    }

    pub fn seek_to_end(&mut self) -> io::Result<u64> {
        self.f.seek(SeekFrom::End(0))
    }

    // We need to wrap `Option` within `Result` to allow for the possibilities of I/O errors as well
    // as missing values occurring.
    pub fn get(&mut self, key: &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(position) => *position,
        };

        let kv = self.get_at(position)?;

        Ok(Some(ByteString::from(kv.value)))
    }

    pub fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
        let mut f = BufReader::new(&mut self.f);
        let mut found: Option<u64, ByteString> = None;

        loop {
            let position = f.seek(SeekFrom::Current(0))?;

            let maybe_kv = ActionKV::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        }
                        _ => return Err(err),
                    };
                }
            };

            if kv.key == target {
                found = Some((position, kv.value));
            }

            // Important to keekp looping until the end of the file,
            // in case the key has been overwritten.
        }

        Ok(found)
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?;

        // key.to_vec() converts the &ByteStr to a ByteString.
        self.index.insert(key.to_vec(), position);
        Ok(())
    }

    pub fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64> {
        // std::io::BufWriter is a type that batches multiple short `write()` calls into fewer actual
        // disk operations. This increases throughput while keeping the application code neater.
        let mut f = BufWriter::new(&mut self.f);

        let key_len = key.len();
        let val_len = value.len();
        let mut tmp = ByteString::with_capacity(key_len + val_len);

        for byte in key {
            tmp.push(*byte);
        }

        for byte in value {
            tmp.push(*byte);
        }

        let checksum = crc32::checksum_ieee(&tmp);

        // We first grab the current position of the offset and store it in `current_position`. We then set
        // the offset to the end of the file and append the new segment.
        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32)?;
        f.write_u32::<LittleEndian>(val_len as u32)?;
        f.write_all(&mut tmp)?;

        Ok(current_position)
    }

    #[inline]
    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key, value)
    }

    #[inline]
    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }
}
