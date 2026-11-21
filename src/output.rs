use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy)]
pub struct UserDay {
    pub person: u64,
    pub day: i32,
    pub events: u32,
    pub sessions: u32,
    pub revenue: i64,
    pub category: u32,
}

#[derive(Clone, Copy, Default)]
pub struct CampaignDay {
    pub campaign: u32,
    pub day: i32,
    pub sessions: u64,
    pub purchasing_sessions: u64,
    pub revenue: i64,
    pub assisted_sessions: u64,
}

const GROUP_ROWS: usize = 65_536;
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
const UTF8: i32 = 0;
const DATE: i32 = 6;
const UINT32: i32 = 13;
const UINT64: i32 = 14;
const PLAIN: i32 = 0;
const RLE: i32 = 3;
const DICTIONARY_PAGE: i32 = 2;
const DATA_PAGE_V2: i32 = 3;
const RLE_DICTIONARY: i32 = 8;
const UNCOMPRESSED: i32 = 0;

#[derive(Clone, Copy)]
struct Field {
    name: &'static [u8],
    physical: i32,
    converted: Option<i32>,
    optional: bool,
}

const fn field(name: &'static [u8], physical: i32, converted: Option<i32>) -> Field {
    Field {
        name,
        physical,
        converted,
        optional: false,
    }
}

const fn optional(mut field: Field) -> Field {
    field.optional = true;
    field
}

const USERS: &[Field] = &[
    field(b"user_id", BYTE_ARRAY, Some(UTF8)),
    field(b"day", INT32, Some(DATE)),
    field(b"sessions", INT32, Some(UINT32)),
    field(b"events", INT32, Some(UINT32)),
    field(b"revenue_cents", INT64, None),
    optional(field(b"top_category", BYTE_ARRAY, Some(UTF8))),
];

const CAMPAIGNS: &[Field] = &[
    field(b"campaign_id", BYTE_ARRAY, Some(UTF8)),
    optional(field(b"channel", BYTE_ARRAY, Some(UTF8))),
    field(b"day", INT32, Some(DATE)),
    field(b"sessions", INT64, Some(UINT64)),
    field(b"purchasing_sessions", INT64, Some(UINT64)),
    field(b"revenue_cents", INT64, None),
    field(b"assisted_sessions", INT64, Some(UINT64)),
];

fn varint(bytes: &mut Vec<u8>, mut number: u64) {
    while number >= 128 {
        bytes.push((number as u8 & 127) | 128);
        number >>= 7;
    }
    bytes.push(number as u8);
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum Wire {
    False = 2,
    I32 = 5,
    I64 = 6,
    Bytes = 8,
    List = 9,
    Struct = 12,
}

// The Parquet footer and page headers are Thrift compact messages.
struct Compact<'a> {
    bytes: &'a mut Vec<u8>,
    prior: u8,
}

impl Compact<'_> {
    fn field(&mut self, id: u8, wire: Wire) {
        let delta = id.wrapping_sub(self.prior);
        if (1..=15).contains(&delta) {
            self.bytes.push((delta << 4) | wire as u8);
        } else {
            self.bytes.push(wire as u8);
            self.integer(id.into());
        }
        self.prior = id;
    }

    fn integer(&mut self, number: i64) {
        varint(self.bytes, ((number << 1) ^ (number >> 63)) as u64);
    }

    fn int(&mut self, id: u8, number: i32) {
        self.field(id, Wire::I32);
        self.integer(number.into());
    }

    fn long(&mut self, id: u8, number: i64) {
        self.field(id, Wire::I64);
        self.integer(number);
    }

    fn bytes(&mut self, bytes: &[u8]) {
        varint(self.bytes, bytes.len() as u64);
        self.bytes.extend_from_slice(bytes);
    }

    fn text(&mut self, id: u8, bytes: &[u8]) {
        self.field(id, Wire::Bytes);
        self.bytes(bytes);
    }

    fn list(&mut self, id: u8, kind: Wire, len: usize) {
        self.field(id, Wire::List);
        self.bytes.push((len.min(15) as u8) << 4 | kind as u8);
        if len >= 15 {
            varint(self.bytes, len as u64);
        }
    }

    fn item(&mut self, write: impl FnOnce(&mut Compact<'_>)) {
        write(&mut Compact {
            bytes: self.bytes,
            prior: 0,
        });
        self.bytes.push(0);
    }

    fn object(&mut self, id: u8, write: impl FnOnce(&mut Compact<'_>)) {
        self.field(id, Wire::Struct);
        self.item(write);
    }
}

#[derive(Clone, Copy)]
struct Column {
    offset: i64,
    dictionary: Option<i64>,
    bytes: i64,
}

struct Group {
    rows: i64,
    columns: Vec<Column>,
}

fn checked_len(len: usize) -> io::Result<i32> {
    i32::try_from(len).map_err(|_| io::Error::other("Parquet page exceeds its 32-bit size limit"))
}

fn bit_width(entries: usize) -> usize {
    usize::BITS as usize - (entries.saturating_sub(1).max(1)).leading_zeros() as usize
}

fn bitpack(out: &mut Vec<u8>, mut ids: impl Iterator<Item = u32>, count: usize, width: usize) {
    let groups = count.div_ceil(8);
    out.push(width as u8);
    varint(out, (groups as u64) * 2 + 1);
    for _ in 0..groups {
        let mut packed = 0u64;
        for offset in 0..8 {
            packed |= u64::from(ids.next().unwrap_or(0)) << (offset * width);
        }
        out.extend_from_slice(&packed.to_le_bytes()[..width]);
    }
}

struct Part {
    file: BufWriter<File>,
    path: PathBuf,
    temporary: PathBuf,
    offset: i64,
    schema: &'static [Field],
    groups: Vec<Group>,
    header: Vec<u8>,
    definition: Vec<u8>,
    values: Vec<u8>,
    runs: Vec<u32>,
}

impl Part {
    fn open(output: &Path, table: &str, part: usize, schema: &'static [Field]) -> io::Result<Self> {
        let table = output.join(table);
        fs::create_dir_all(&table)?;
        let path = table.join(format!("part-{part:04}.parquet"));
        let temporary = table.join(format!(".part-{part:04}-{}.tmp", std::process::id()));
        let mut file = BufWriter::with_capacity(16 * 1024, File::create(&temporary)?);
        file.write_all(PARQUET_MAGIC)?;
        Ok(Self {
            file,
            path,
            temporary,
            offset: PARQUET_MAGIC.len() as i64,
            schema,
            groups: Vec::new(),
            header: Vec::new(),
            definition: Vec::new(),
            values: Vec::new(),
            runs: Vec::new(),
        })
    }

    fn page(&mut self, rows: usize, nulls: usize) -> io::Result<Column> {
        self.data_page(rows, nulls, None)
    }

    fn write_dictionary(&mut self, entries: usize) -> io::Result<i64> {
        let length = checked_len(self.values.len())?;
        self.header.clear();
        let mut header = Compact {
            bytes: &mut self.header,
            prior: 0,
        };
        header.int(1, DICTIONARY_PAGE);
        header.int(2, length);
        header.int(3, length);
        header.object(7, |dict| {
            dict.int(1, entries as i32);
            dict.int(2, PLAIN);
        });
        self.header.push(0);
        let offset = self.offset;
        self.file.write_all(&self.header)?;
        self.file.write_all(&self.values)?;
        self.offset += i64::from(length) + self.header.len() as i64;
        self.values.clear();
        Ok(offset)
    }

    fn data_page(
        &mut self,
        rows: usize,
        nulls: usize,
        dictionary: Option<i64>,
    ) -> io::Result<Column> {
        let length = checked_len(self.values.len() + self.definition.len())?;
        self.header.clear();
        let mut header = Compact {
            bytes: &mut self.header,
            prior: 0,
        };
        header.int(1, DATA_PAGE_V2);
        header.int(2, length);
        header.int(3, length);
        header.object(8, |page| {
            page.int(1, rows as i32); // Number of values, including nulls.
            page.int(2, nulls as i32);
            page.int(3, rows as i32);
            page.int(
                4,
                if dictionary.is_some() {
                    RLE_DICTIONARY
                } else {
                    PLAIN
                },
            );
            page.int(5, self.definition.len() as i32);
            page.int(6, 0); // Flat schema has no repetition levels.
            page.field(7, Wire::False);
        });
        self.header.push(0);
        self.file.write_all(&self.header)?;
        self.file.write_all(&self.definition)?;
        self.file.write_all(&self.values)?;
        let bytes = i64::from(length) + self.header.len() as i64;
        let column = Column {
            offset: self.offset,
            dictionary,
            bytes: bytes + self.offset - dictionary.unwrap_or(self.offset),
        };
        self.offset += bytes;
        self.values.clear();
        self.definition.clear();
        Ok(column)
    }

    fn integers<const WIDTH: usize>(
        &mut self,
        count: usize,
        values: impl Iterator<Item = [u8; WIDTH]>,
    ) -> io::Result<Column> {
        self.values.reserve(count * WIDTH);
        for value in values {
            self.values.extend_from_slice(&value);
        }
        self.page(count, 0)
    }

    fn text<'a>(
        &mut self,
        count: usize,
        values: impl Iterator<Item = Option<&'a [u8]>>,
        optional: bool,
    ) -> io::Result<Column> {
        let count_bytes = count.div_ceil(8);
        let level_start = if optional {
            // One bit-packed run of definition levels. A final partial byte
            // has zero padding, which readers discard using num_values.
            varint(&mut self.definition, ((count_bytes as u64) << 1) | 1);
            let level_start = self.definition.len();
            self.definition.resize(level_start + count_bytes, 0);
            level_start
        } else {
            0
        };
        let mut nulls = 0;
        for (index, value) in values.enumerate() {
            if let Some(value) = value {
                if optional {
                    self.definition[level_start + index / 8] |= 1 << (index % 8);
                }
                let bytes = checked_len(value.len())?;
                self.values.extend_from_slice(&bytes.to_le_bytes());
                self.values.extend_from_slice(value);
            } else {
                debug_assert!(optional);
                nulls += 1;
            }
        }
        self.page(count, nulls)
    }

    fn compact_names(
        &mut self,
        rows: &[UserDay],
        append: &impl Fn(u64, &mut Vec<u8>),
    ) -> io::Result<(usize, Column)> {
        self.runs.clear();
        let mut count = 0;
        while count < rows.len() {
            let person = rows[count].person;
            let limit = count
                + rows[count..]
                    .iter()
                    .take_while(|row| row.person == person)
                    .count();
            let start = self.values.len();
            self.values.extend_from_slice(&[0; 4]);
            append(person, &mut self.values);
            let len = checked_len(self.values.len() - start - 4)?;
            self.values[start..start + 4].copy_from_slice(&len.to_le_bytes());
            self.runs.push((limit - count) as u32);
            count = limit;
            if self.values.len() >= 16 * 1024 * 1024 {
                break;
            }
        }
        let entries = self.runs.len();
        let dictionary = self.write_dictionary(entries)?;
        let width = bit_width(entries);
        let bytes = width.div_ceil(8);
        self.values.push(width as u8);
        // Aggregation emits a person's consecutive days together. Encode
        // the original spelling once and repeat its index for that run.
        for (id, &count) in self.runs.iter().enumerate() {
            varint(&mut self.values, 2 * u64::from(count));
            self.values
                .extend_from_slice(&(id as u32).to_le_bytes()[..bytes]);
        }
        self.data_page(count, 0, Some(dictionary))
            .map(|column| (count, column))
    }

    fn dates(&mut self, rows: &[UserDay]) -> io::Result<Column> {
        let (min, max) = rows.iter().fold((i32::MAX, i32::MIN), |(min, max), row| {
            (min.min(row.day), max.max(row.day))
        });
        let entries = i64::from(max) - i64::from(min) + 1;
        if entries > 64 {
            return self.integers(rows.len(), rows.iter().map(|r| r.day.to_le_bytes()));
        }
        for day in min..=max {
            self.values.extend_from_slice(&day.to_le_bytes());
        }
        let dictionary = self.write_dictionary(entries as usize)?;
        bitpack(
            &mut self.values,
            rows.iter().map(|r| (r.day - min) as u32),
            rows.len(),
            bit_width(entries as usize),
        );
        self.data_page(rows.len(), 0, Some(dictionary))
    }

    fn categories(&mut self, rows: &[UserDay], categories: &[Vec<u8>]) -> io::Result<Column> {
        let entries = categories.len().saturating_sub(1);
        let non_null = rows.iter().filter(|r| r.category != 0).count();
        if !(1..=256).contains(&entries) || non_null == 0 {
            return self.text(
                rows.len(),
                rows.iter()
                    .map(|r| (r.category != 0).then(|| categories[r.category as usize].as_slice())),
                true,
            );
        }
        for category in &categories[1..] {
            let length = checked_len(category.len())?;
            self.values.extend_from_slice(&length.to_le_bytes());
            self.values.extend_from_slice(category);
        }
        let dictionary = self.write_dictionary(entries)?;
        let bytes = rows.len().div_ceil(8);
        varint(&mut self.definition, ((bytes as u64) << 1) | 1);
        let start = self.definition.len();
        self.definition.resize(start + bytes, 0);
        for (i, row) in rows.iter().enumerate() {
            if row.category != 0 {
                self.definition[start + i / 8] |= 1 << (i % 8);
            }
        }
        bitpack(
            &mut self.values,
            rows.iter()
                .filter(|r| r.category != 0)
                .map(|r| r.category - 1),
            non_null,
            bit_width(entries),
        );
        self.data_page(rows.len(), rows.len() - non_null, Some(dictionary))
    }

    fn group(&mut self, rows: usize, columns: Vec<Column>) {
        self.groups.push(Group {
            rows: rows as i64,
            columns,
        });
    }

    fn finish(mut self) -> io::Result<()> {
        let footer = footer(self.schema, &self.groups);
        let length = u32::try_from(footer.len())
            .map_err(|_| io::Error::other("Parquet footer exceeds its size limit"))?;
        self.file.write_all(&footer)?;
        self.file.write_all(&length.to_le_bytes())?;
        self.file.write_all(PARQUET_MAGIC)?;
        self.file.flush()?;
        drop(self.file);
        fs::rename(self.temporary, self.path)
    }
}

fn footer(schema: &[Field], groups: &[Group]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(512 + groups.len() * 192);
    let mut footer = Compact {
        bytes: &mut bytes,
        prior: 0,
    };
    footer.int(1, 1); // FileMetaData.version
    footer.list(2, Wire::Struct, schema.len() + 1);
    footer.item(|root| {
        root.text(4, b"rows");
        root.int(5, schema.len() as i32);
    });
    for field in schema {
        footer.item(|leaf| {
            leaf.int(1, field.physical);
            leaf.int(3, i32::from(field.optional));
            leaf.text(4, field.name);
            if let Some(converted) = field.converted {
                leaf.int(6, converted);
            }
        });
    }
    footer.long(3, groups.iter().map(|group| group.rows).sum());
    footer.list(4, Wire::Struct, groups.len());
    for group in groups {
        footer.item(|out| {
            out.list(1, Wire::Struct, schema.len());
            for (&field, column) in schema.iter().zip(&group.columns) {
                out.item(|chunk| {
                    chunk.long(2, column.dictionary.unwrap_or(column.offset));
                    chunk.object(3, |meta| {
                        meta.int(1, field.physical);
                        meta.list(
                            2,
                            Wire::I32,
                            1 + usize::from(field.optional)
                                + usize::from(column.dictionary.is_some()),
                        );
                        meta.integer(PLAIN.into());
                        if field.optional {
                            meta.integer(RLE.into());
                        }
                        if column.dictionary.is_some() {
                            meta.integer(RLE_DICTIONARY.into());
                        }
                        meta.list(3, Wire::Bytes, 1);
                        meta.bytes(field.name);
                        meta.int(4, UNCOMPRESSED);
                        meta.long(5, group.rows);
                        meta.long(6, column.bytes);
                        meta.long(7, column.bytes);
                        meta.long(9, column.offset);
                        if let Some(dictionary_offset) = column.dictionary {
                            meta.long(11, dictionary_offset);
                        }
                    });
                });
            }
            out.long(2, group.columns.iter().map(|column| column.bytes).sum());
            out.long(3, group.rows);
        });
    }
    bytes.push(0);
    bytes
}

pub struct UserWriter(Part);

impl UserWriter {
    pub fn open(output: &Path, part: usize) -> io::Result<Self> {
        Part::open(output, "user_daily", part, USERS).map(Self)
    }

    pub fn append(
        &mut self,
        mut rows: &[UserDay],
        append_user: &impl Fn(u64, &mut Vec<u8>),
        categories: &[Vec<u8>],
    ) -> io::Result<()> {
        let file = &mut self.0;
        while !rows.is_empty() {
            let page = &rows[..rows.len().min(GROUP_ROWS)];
            let (count, names) = file.compact_names(page, &append_user)?;
            let group = &rows[..count];
            let columns = vec![
                names,
                file.dates(group)?,
                file.integers(count, group.iter().map(|row| row.sessions.to_le_bytes()))?,
                file.integers(count, group.iter().map(|row| row.events.to_le_bytes()))?,
                file.integers(count, group.iter().map(|row| row.revenue.to_le_bytes()))?,
                file.categories(group, categories)?,
            ];
            file.group(count, columns);
            rows = &rows[count..];
        }
        Ok(())
    }

    pub fn finish(self) -> io::Result<()> {
        self.0.finish()
    }
}

pub fn write_users(
    output: &Path,
    part: usize,
    rows: &[UserDay],
    append_user: impl Fn(u64, &mut Vec<u8>),
    categories: &[Vec<u8>],
) -> io::Result<()> {
    let mut writer = UserWriter::open(output, part)?;
    writer.append(rows, &append_user, categories)?;
    writer.finish()
}

pub fn write_campaigns(
    output: &Path,
    rows: &[CampaignDay],
    ids: &[Vec<u8>],
    channels: &[Vec<u8>],
) -> io::Result<()> {
    let mut file = Part::open(output, "campaign_daily", 0, CAMPAIGNS)?;
    for group in rows.chunks(GROUP_ROWS) {
        let count = group.len();
        let columns = vec![
            file.text(
                count,
                group
                    .iter()
                    .map(|row| Some(ids[row.campaign as usize].as_slice())),
                false,
            )?,
            file.text(
                count,
                group.iter().map(|row| {
                    let value = channels[row.campaign as usize].as_slice();
                    (!value.is_empty()).then_some(value)
                }),
                true,
            )?,
            file.integers(count, group.iter().map(|row| row.day.to_le_bytes()))?,
            file.integers(count, group.iter().map(|row| row.sessions.to_le_bytes()))?,
            file.integers(
                count,
                group
                    .iter()
                    .map(|row| row.purchasing_sessions.to_le_bytes()),
            )?,
            file.integers(count, group.iter().map(|row| row.revenue.to_le_bytes()))?,
            file.integers(
                count,
                group.iter().map(|row| row.assisted_sessions.to_le_bytes()),
            )?,
        ];
        file.group(count, columns);
    }
    file.finish()
}
