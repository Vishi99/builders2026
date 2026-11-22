//! A small Parquet writer: flat schema, one row group per file part, PLAIN
//! values (dictionary for low-cardinality strings), uncompressed V1 pages.

use std::io::{self, Write};

const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const ENC_RLE_DICT: i32 = 8;
const PAGE_DATA: i32 = 0;
const PAGE_DICT: i32 = 2;

struct Thrift<'a> {
    out: &'a mut Vec<u8>,
    last: Vec<i16>,
}

impl<'a> Thrift<'a> {
    fn new(out: &'a mut Vec<u8>) -> Self {
        Thrift { out, last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.out.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.out.push(v as u8);
    }
    fn header(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let delta = id - last;
        if (1..=15).contains(&delta) {
            self.out.push(((delta as u8) << 4) | ty);
        } else {
            self.out.push(ty);
            let z = ((id as i64) << 1) ^ ((id as i64) >> 63);
            self.varint(z as u64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.header(id, 5);
        let z = ((v as i64) << 1) ^ ((v as i64) >> 63);
        self.varint(z as u64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.header(id, 6);
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn bin(&mut self, id: i16, b: &[u8]) {
        self.header(id, 8);
        self.varint(b.len() as u64);
        self.out.extend_from_slice(b);
    }
    fn begin(&mut self, id: i16) {
        self.header(id, 12);
        self.last.push(0);
    }
    fn end(&mut self) {
        self.out.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, ty: u8, n: usize) {
        self.header(id, 9);
        if n < 15 {
            self.out.push(((n as u8) << 4) | ty);
        } else {
            self.out.push(0xf0 | ty);
            self.varint(n as u64);
        }
    }
    fn elem_begin(&mut self) {
        self.last.push(0);
    }
    fn raw_i32(&mut self, v: i32) {
        let z = ((v as i64) << 1) ^ ((v as i64) >> 63);
        self.varint(z as u64);
    }
}

pub enum Col<'a> {
    /// Required strings, given as offsets into a byte buffer.
    Str { offs: &'a [u32], data: &'a [u8] },
    /// Optional strings from a small dictionary; `ids[i] == 0` is null, else dict[ids[i]-1].
    DictStr { ids: &'a [u32], dict: &'a [Vec<u8>], optional: bool },
    I32 { v: &'a [i32], date: bool },
    I64 { v: &'a [i64] },
}

pub struct Field<'a> {
    pub name: &'a str,
    pub col: Col<'a>,
}

struct Meta {
    ty: i32,
    data_offset: i64,
    dict_offset: Option<i64>,
    size: i64,
    encodings: Vec<i32>,
}

fn bit_width(max: u32) -> u8 {
    (32 - max.leading_zeros()) as u8
}

/// RLE/bit-packed hybrid as one bit-packed run.
fn hybrid(out: &mut Vec<u8>, vals: impl Iterator<Item = u32>, n: usize, width: u8) {
    let groups = n.div_ceil(8);
    let mut v = ((groups as u64) << 1) | 1;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
    let w = width as usize;
    let start = out.len();
    out.resize(start + groups * w, 0);
    let buf = &mut out[start..];
    for (i, x) in vals.enumerate() {
        let bit = i * w;
        let mut val = (x as u64) << (bit & 7);
        let mut byte = bit >> 3;
        while val != 0 {
            buf[byte] |= val as u8;
            val >>= 8;
            byte += 1;
        }
    }
}

fn page_header(out: &mut Vec<u8>, ptype: i32, size: usize, n: usize, encoding: i32, dict: bool) {
    let mut t = Thrift::new(out);
    t.i32(1, ptype);
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    if dict {
        t.begin(7);
        t.i32(1, n as i32);
        t.i32(2, ENC_PLAIN);
        t.end();
    } else {
        t.begin(5);
        t.i32(1, n as i32);
        t.i32(2, encoding);
        t.i32(3, ENC_RLE);
        t.i32(4, ENC_RLE);
        t.end();
    }
    t.out.push(0);
}

/// One column chunk appended to `out`; offsets in the result are relative to
/// the start of `out`.
fn encode_column(out: &mut Vec<u8>, col: &Col, rows: usize) -> Meta {
    let mut metas: Vec<Meta> = Vec::new();
    let mut body: Vec<u8> = Vec::new();
    let f = Field { name: "", col: match col {
        Col::Str { offs, data } => Col::Str { offs, data },
        Col::DictStr { ids, dict, optional } => Col::DictStr { ids, dict, optional: *optional },
        Col::I32 { v, date } => Col::I32 { v, date: *date },
        Col::I64 { v } => Col::I64 { v },
    } };
        match &f.col {
            Col::Str { offs, data } => {
                for i in 0..rows {
                    let s = &data[offs[i] as usize..offs[i + 1] as usize];
                    body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    body.extend_from_slice(s);
                }
                let off = out.len() as i64;
                page_header(out, PAGE_DATA, body.len(), rows, ENC_PLAIN, false);
                out.extend_from_slice(&body);
                metas.push(Meta {
                    ty: BYTE_ARRAY,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
            Col::DictStr { ids, optional: true, .. } if ids.iter().all(|&x| x == 0) => {
                let mut lv = Vec::new();
                hybrid(&mut lv, ids.iter().map(|_| 0u32), rows, 1);
                body.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                body.extend_from_slice(&lv);
                let off = out.len() as i64;
                page_header(out, PAGE_DATA, body.len(), rows, ENC_PLAIN, false);
                out.extend_from_slice(&body);
                metas.push(Meta {
                    ty: BYTE_ARRAY,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
            Col::DictStr { ids, dict, optional } => {
                let dict_off = out.len() as i64;
                body.clear();
                for s in dict.iter() {
                    body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    body.extend_from_slice(s);
                }
                page_header(out, PAGE_DICT, body.len(), dict.len(), ENC_PLAIN, true);
                out.extend_from_slice(&body);
                body.clear();
                let nonnull = ids.iter().filter(|&&x| x != 0).count();
                if *optional {
                    let mut lv = Vec::new();
                    hybrid(&mut lv, ids.iter().map(|&x| (x != 0) as u32), rows, 1);
                    body.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                    body.extend_from_slice(&lv);
                }
                let width = bit_width((dict.len() as u32).saturating_sub(1)).max(1);
                body.push(width);
                hybrid(&mut body, ids.iter().filter(|&&x| x != 0).map(|&x| x - 1), nonnull, width);
                let data_off = out.len() as i64;
                page_header(out, PAGE_DATA, body.len(), rows, ENC_RLE_DICT, false);
                out.extend_from_slice(&body);
                metas.push(Meta {
                    ty: BYTE_ARRAY,
                    data_offset: data_off,
                    dict_offset: Some(dict_off),
                    size: out.len() as i64 - dict_off,
                    encodings: vec![ENC_PLAIN, ENC_RLE, ENC_RLE_DICT],
                });
            }
            Col::I32 { v, .. } => {
                let off = out.len() as i64;
                page_header(out, PAGE_DATA, rows * 4, rows, ENC_PLAIN, false);
                let bytes = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, rows * 4) };
                out.extend_from_slice(bytes);
                metas.push(Meta {
                    ty: INT32,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
            Col::I64 { v } => {
                let off = out.len() as i64;
                page_header(out, PAGE_DATA, rows * 8, rows, ENC_PLAIN, false);
                let bytes = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, rows * 8) };
                out.extend_from_slice(bytes);
                metas.push(Meta {
                    ty: INT64,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
        }
    metas.pop().unwrap()
}

/// Serialize a whole file with one row group.
pub fn encode(fields: &[Field], rows: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows * 48 + 4096);
    out.extend_from_slice(b"PAR1");
    let mut metas = Vec::new();
    let mut body = Vec::new();
    for f in fields {
        body.clear();
        match &f.col {
            Col::Str { offs, data } => {
                for i in 0..rows {
                    let s = &data[offs[i] as usize..offs[i + 1] as usize];
                    body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    body.extend_from_slice(s);
                }
                let off = out.len() as i64;
                page_header(&mut out, PAGE_DATA, body.len(), rows, ENC_PLAIN, false);
                out.extend_from_slice(&body);
                metas.push(Meta {
                    ty: BYTE_ARRAY,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
            Col::DictStr { ids, optional: true, .. } if ids.iter().all(|&x| x == 0) => {
                let mut lv = Vec::new();
                hybrid(&mut lv, ids.iter().map(|_| 0u32), rows, 1);
                body.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                body.extend_from_slice(&lv);
                let off = out.len() as i64;
                page_header(&mut out, PAGE_DATA, body.len(), rows, ENC_PLAIN, false);
                out.extend_from_slice(&body);
                metas.push(Meta {
                    ty: BYTE_ARRAY,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
            Col::DictStr { ids, dict, optional } => {
                let dict_off = out.len() as i64;
                body.clear();
                for s in dict.iter() {
                    body.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    body.extend_from_slice(s);
                }
                page_header(&mut out, PAGE_DICT, body.len(), dict.len(), ENC_PLAIN, true);
                out.extend_from_slice(&body);
                body.clear();
                let nonnull = ids.iter().filter(|&&x| x != 0).count();
                if *optional {
                    let mut lv = Vec::new();
                    hybrid(&mut lv, ids.iter().map(|&x| (x != 0) as u32), rows, 1);
                    body.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                    body.extend_from_slice(&lv);
                }
                let width = bit_width((dict.len() as u32).saturating_sub(1)).max(1);
                body.push(width);
                hybrid(&mut body, ids.iter().filter(|&&x| x != 0).map(|&x| x - 1), nonnull, width);
                let data_off = out.len() as i64;
                page_header(&mut out, PAGE_DATA, body.len(), rows, ENC_RLE_DICT, false);
                out.extend_from_slice(&body);
                metas.push(Meta {
                    ty: BYTE_ARRAY,
                    data_offset: data_off,
                    dict_offset: Some(dict_off),
                    size: out.len() as i64 - dict_off,
                    encodings: vec![ENC_PLAIN, ENC_RLE, ENC_RLE_DICT],
                });
            }
            Col::I32 { v, .. } => {
                let off = out.len() as i64;
                page_header(&mut out, PAGE_DATA, rows * 4, rows, ENC_PLAIN, false);
                let bytes = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, rows * 4) };
                out.extend_from_slice(bytes);
                metas.push(Meta {
                    ty: INT32,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
            Col::I64 { v } => {
                let off = out.len() as i64;
                page_header(&mut out, PAGE_DATA, rows * 8, rows, ENC_PLAIN, false);
                let bytes = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, rows * 8) };
                out.extend_from_slice(bytes);
                metas.push(Meta {
                    ty: INT64,
                    data_offset: off,
                    dict_offset: None,
                    size: out.len() as i64 - off,
                    encodings: vec![ENC_PLAIN, ENC_RLE],
                });
            }
        }
    }
    // Footer: FileMetaData.
    let mut footer = Vec::new();
    {
        let mut t = Thrift::new(&mut footer);
        t.i32(1, 1);
        t.list(2, 12, fields.len() + 1);
        t.elem_begin();
        t.bin(4, b"schema");
        t.i32(5, fields.len() as i32);
        t.end();
        for f in fields {
            t.elem_begin();
            let (ty, conv, optional) = match &f.col {
                Col::Str { .. } => (BYTE_ARRAY, Some(CONV_UTF8), false),
                Col::DictStr { optional, .. } => (BYTE_ARRAY, Some(CONV_UTF8), *optional),
                Col::I32 { date, .. } => (INT32, if *date { Some(CONV_DATE) } else { None }, false),
                Col::I64 { .. } => (INT64, None, false),
            };
            t.i32(1, ty);
            t.i32(3, if optional { 1 } else { 0 });
            t.bin(4, f.name.as_bytes());
            if let Some(c) = conv {
                t.i32(6, c);
            }
            if conv == Some(CONV_UTF8) {
                // LogicalType: STRING
                t.begin(10);
                t.begin(1);
                t.end();
                t.end();
            } else if conv == Some(CONV_DATE) {
                t.begin(10);
                t.begin(6);
                t.end();
                t.end();
            }
            t.end();
        }
        t.i64(3, rows as i64);
        t.list(4, 12, 1);
        // RowGroup
        t.elem_begin();
        t.list(1, 12, fields.len());
        let mut total = 0i64;
        for (f, m) in fields.iter().zip(&metas) {
            t.elem_begin();
            t.i64(2, m.dict_offset.unwrap_or(m.data_offset));
            t.begin(3);
            t.i32(1, m.ty);
            t.list(2, 5, m.encodings.len());
            for &e in &m.encodings {
                t.raw_i32(e);
            }
            t.list(3, 8, 1);
            t.varint(f.name.len() as u64);
            t.out.extend_from_slice(f.name.as_bytes());
            t.i32(4, 0);
            t.i64(5, rows as i64);
            t.i64(6, m.size);
            t.i64(7, m.size);
            t.i64(9, m.data_offset);
            if let Some(d) = m.dict_offset {
                t.i64(11, d);
            }
            t.end();
            t.end();
            total += m.size;
        }
        t.i64(2, total);
        t.i64(3, rows as i64);
        t.end();
        t.out.push(0);
    }
    out.extend_from_slice(&footer);
    out.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    out
}

pub fn write(path: &std::path::Path, bytes: &[u8]) -> io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    f.write_all(bytes)
}

/// Incremental single-row-group writer that appends column chunks directly.
pub struct Writer {
    pub out: Vec<u8>,
    metas: Vec<Meta>,
    schema: Vec<(&'static str, i32, Option<i32>, bool)>,
    rows: usize,
}

impl Writer {
    pub fn new(rows: usize, capacity: usize) -> Writer {
        let mut out = Vec::with_capacity(capacity + 4096);
        out.extend_from_slice(b"PAR1");
        Writer { out, metas: Vec::new(), schema: Vec::new(), rows }
    }

    fn data_page(&mut self, size: usize, encoding: i32) -> i64 {
        let off = self.out.len() as i64;
        page_header(&mut self.out, PAGE_DATA, size, self.rows, encoding, false);
        off
    }

    /// Required INT32 (optionally DATE) column.
    pub fn i32_col(&mut self, name: &'static str, date: bool, vals: impl Iterator<Item = i32>) {
        let off = self.data_page(self.rows * 4, ENC_PLAIN);
        for v in vals {
            self.out.extend_from_slice(&v.to_le_bytes());
        }
        self.metas.push(Meta { ty: INT32, data_offset: off, dict_offset: None, size: self.out.len() as i64 - off, encodings: vec![ENC_PLAIN, ENC_RLE] });
        self.schema.push((name, INT32, if date { Some(CONV_DATE) } else { None }, false));
    }

    pub fn i64_col(&mut self, name: &'static str, vals: impl Iterator<Item = i64>) {
        let off = self.data_page(self.rows * 8, ENC_PLAIN);
        for v in vals {
            self.out.extend_from_slice(&v.to_le_bytes());
        }
        self.metas.push(Meta { ty: INT64, data_offset: off, dict_offset: None, size: self.out.len() as i64 - off, encodings: vec![ENC_PLAIN, ENC_RLE] });
        self.schema.push((name, INT64, None, false));
    }

    /// Required strings: `total` is the sum of value lengths; `put` appends
    /// each value (without its length prefix) and returns its length.
    pub fn str_col(&mut self, name: &'static str, total: usize, mut put: impl FnMut(usize, &mut Vec<u8>)) {
        let off = self.data_page(total + 4 * self.rows, ENC_PLAIN);
        for i in 0..self.rows {
            let at = self.out.len();
            self.out.extend_from_slice(&[0u8; 4]);
            put(i, &mut self.out);
            let l = (self.out.len() - at - 4) as u32;
            self.out[at..at + 4].copy_from_slice(&l.to_le_bytes());
        }
        self.metas.push(Meta { ty: BYTE_ARRAY, data_offset: off, dict_offset: None, size: self.out.len() as i64 - off, encodings: vec![ENC_PLAIN, ENC_RLE] });
        self.schema.push((name, BYTE_ARRAY, Some(CONV_UTF8), false));
    }

    /// Dictionary strings; `ids[i] == 0` is null when `optional`.
    pub fn dict_col(&mut self, name: &'static str, ids: &[u32], dict: &[Vec<u8>], optional: bool) {
        let col = Col::DictStr { ids, dict, optional };
        let start = self.out.len() as i64;
        let mut tmp = Vec::new();
        let m = encode_column(&mut tmp, &col, self.rows);
        let base = start;
        self.out.extend_from_slice(&tmp);
        self.metas.push(Meta {
            ty: m.ty,
            data_offset: m.data_offset + base,
            dict_offset: m.dict_offset.map(|d| d + base),
            size: m.size,
            encodings: m.encodings,
        });
        self.schema.push((name, BYTE_ARRAY, Some(CONV_UTF8), optional));
    }

    pub fn finish(mut self) -> Vec<u8> {
        let mut footer = Vec::new();
        {
            let mut t = Thrift::new(&mut footer);
            t.i32(1, 1);
            t.list(2, 12, self.schema.len() + 1);
            t.elem_begin();
            t.bin(4, b"schema");
            t.i32(5, self.schema.len() as i32);
            t.end();
            for &(name, ty, conv, optional) in &self.schema {
                t.elem_begin();
                t.i32(1, ty);
                t.i32(3, if optional { 1 } else { 0 });
                t.bin(4, name.as_bytes());
                if let Some(c) = conv {
                    t.i32(6, c);
                }
                if conv == Some(CONV_UTF8) {
                    t.begin(10);
                    t.begin(1);
                    t.end();
                    t.end();
                } else if conv == Some(CONV_DATE) {
                    t.begin(10);
                    t.begin(6);
                    t.end();
                    t.end();
                }
                t.end();
            }
            t.i64(3, self.rows as i64);
            t.list(4, 12, 1);
            t.elem_begin();
            t.list(1, 12, self.schema.len());
            let mut total = 0i64;
            for (&(name, _, _, _), m) in self.schema.iter().zip(&self.metas) {
                t.elem_begin();
                t.i64(2, m.dict_offset.unwrap_or(m.data_offset));
                t.begin(3);
                t.i32(1, m.ty);
                t.list(2, 5, m.encodings.len());
                for &e in &m.encodings {
                    t.raw_i32(e);
                }
                t.list(3, 8, 1);
                t.varint(name.len() as u64);
                t.out.extend_from_slice(name.as_bytes());
                t.i32(4, 0);
                t.i64(5, self.rows as i64);
                t.i64(6, m.size);
                t.i64(7, m.size);
                t.i64(9, m.data_offset);
                if let Some(d) = m.dict_offset {
                    t.i64(11, d);
                }
                t.end();
                t.end();
                total += m.size;
            }
            t.i64(2, total);
            t.i64(3, self.rows as i64);
            t.end();
            t.out.push(0);
        }
        self.out.extend_from_slice(&footer);
        self.out.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        self.out.extend_from_slice(b"PAR1");
        self.out
    }
}
