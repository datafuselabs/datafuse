// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::BufRead;
use std::io::Cursor;
use std::ops::Not;

use aho_corasick::AhoCorasick;
use bstr::ByteSlice;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::serialize::read_decimal_with_size;
use common_expression::serialize::uniform_date;
use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::date::check_date;
use common_expression::types::decimal::Decimal;
use common_expression::types::decimal::DecimalColumnBuilder;
use common_expression::types::decimal::DecimalSize;
use common_expression::types::nullable::NullableColumnBuilder;
use common_expression::types::number::Number;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::timestamp::check_timestamp;
use common_expression::types::AnyType;
use common_expression::types::NumberColumnBuilder;
use common_expression::with_decimal_type;
use common_expression::with_number_mapped_type;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::constants::FALSE_BYTES_LOWER;
use common_io::constants::INF_BYTES_LOWER;
use common_io::constants::NAN_BYTES_LOWER;
use common_io::constants::NULL_BYTES_UPPER;
use common_io::constants::TRUE_BYTES_LOWER;
use common_io::cursor_ext::BufferReadDateTimeExt;
use common_io::cursor_ext::BufferReadStringExt;
use common_io::cursor_ext::DateTimeResType;
use common_io::cursor_ext::ReadBytesExt;
use common_io::cursor_ext::ReadCheckPointExt;
use common_io::cursor_ext::ReadNumberExt;
use common_io::prelude::FormatSettings;
use jsonb::parse_value;
use lexical_core::FromLexical;
use num::cast::AsPrimitive;
use once_cell::sync::Lazy;

use crate::CommonSettings;
use crate::FieldDecoder;

#[derive(Clone)]
pub struct FastFieldDecoderValues {
    pub common_settings: CommonSettings,
}

impl FieldDecoder for FastFieldDecoderValues {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FastFieldDecoderValues {
    pub fn create_for_insert(format: FormatSettings) -> Self {
        FastFieldDecoderValues {
            common_settings: CommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_bytes: NULL_BYTES_UPPER.as_bytes().to_vec(),
                nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
                inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
                timezone: format.timezone,
                disable_variant_check: false,
            },
        }
    }

    fn common_settings(&self) -> &CommonSettings {
        &self.common_settings
    }

    fn ignore_field_end<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>) -> bool {
        reader.ignore_white_spaces();
        matches!(reader.peek(), None | Some(',') | Some(')') | Some(']'))
    }

    fn match_bytes<R: AsRef<[u8]>>(&self, reader: &mut Cursor<R>, bs: &[u8]) -> bool {
        let pos = reader.checkpoint();
        if reader.ignore_bytes(bs) && self.ignore_field_end(reader) {
            true
        } else {
            reader.rollback(pos);
            false
        }
    }

    fn pop_inner_values(&self, column: &mut ColumnBuilder, size: usize) {
        for _ in 0..size {
            let _ = column.pop();
        }
    }

    pub fn read_field<R: AsRef<[u8]>>(
        &self,
        column: &mut ColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        match column {
            ColumnBuilder::Null { len } => self.read_null(len, reader),
            ColumnBuilder::Nullable(c) => self.read_nullable(c, reader, positions),
            ColumnBuilder::Boolean(c) => self.read_bool(c, reader),
            ColumnBuilder::Number(c) => with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumnBuilder::NUM_TYPE(c) => {
                    if NUM_TYPE::FLOATING {
                        self.read_float(c, reader)
                    } else {
                        self.read_int(c, reader)
                    }
                }
            }),
            ColumnBuilder::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumnBuilder::DECIMAL_TYPE(c, size) => self.read_decimal(c, *size, reader),
            }),
            ColumnBuilder::Date(c) => self.read_date(c, reader, positions),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, reader, positions),
            ColumnBuilder::String(c) => self.read_string(c, reader, positions),
            ColumnBuilder::Array(c) => self.read_array(c, reader, positions),
            ColumnBuilder::Map(c) => self.read_map(c, reader, positions),
            ColumnBuilder::Bitmap(_) => Err(ErrorCode::Unimplemented("not implement")),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, reader, positions),
            ColumnBuilder::Variant(c) => self.read_variant(c, reader, positions),
            _ => unimplemented!(),
        }
    }

    fn read_bool<R: AsRef<[u8]>>(
        &self,
        column: &mut MutableBitmap,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        if self.match_bytes(reader, &self.common_settings().true_bytes) {
            column.push(true);
            Ok(())
        } else if self.match_bytes(reader, &self.common_settings().false_bytes) {
            column.push(false);
            Ok(())
        } else {
            let err_msg = format!(
                "Incorrect boolean value, expect {} or {}",
                self.common_settings().true_bytes.to_str().unwrap(),
                self.common_settings().false_bytes.to_str().unwrap()
            );
            Err(ErrorCode::BadBytes(err_msg))
        }
    }

    fn read_null<R: AsRef<[u8]>>(&self, len: &mut usize, _reader: &mut Cursor<R>) -> Result<()> {
        *len += 1;
        Ok(())
    }

    fn read_nullable<R: AsRef<[u8]>>(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        if reader.eof() || reader.ignore_bytes(b"NULL") || reader.ignore_bytes(b"null") {
            column.push_null();
        } else {
            self.read_field(&mut column.builder, reader, positions)?;
            column.validity.push(true);
        }
        Ok(())
    }

    fn read_int<T, R: AsRef<[u8]>>(&self, column: &mut Vec<T>, reader: &mut Cursor<R>) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical,
    {
        let v: T::Native = reader.read_int_text()?;
        column.push(v.into());
        Ok(())
    }

    fn read_float<T, R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<T>,
        reader: &mut Cursor<R>,
    ) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical,
    {
        let v: T::Native = reader.read_float_text()?;
        column.push(v.into());
        Ok(())
    }

    fn read_decimal<R: AsRef<[u8]>, D: Decimal>(
        &self,
        column: &mut Vec<D>,
        size: DecimalSize,
        reader: &mut Cursor<R>,
    ) -> Result<()> {
        let buf = reader.remaining_slice();
        let (n, n_read) = read_decimal_with_size(buf, size, false)?;
        column.push(n);
        reader.consume(n_read);
        Ok(())
    }

    fn read_string_inner<R: AsRef<[u8]>>(
        &self,
        reader: &mut Cursor<R>,
        out_buf: &mut Vec<u8>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.fast_read_quoted_text(out_buf, positions)?;
        Ok(())
    }

    fn read_string<R: AsRef<[u8]>>(
        &self,
        column: &mut StringColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        self.read_string_inner(reader, &mut column.data, positions)?;
        column.commit_row();
        Ok(())
    }

    fn read_date<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i32>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        let mut buffer_readr = Cursor::new(&buf);
        let date = buffer_readr.read_date_text(&self.common_settings().timezone)?;
        let days = uniform_date(date);
        check_date(days as i64)?;
        column.push(days);
        Ok(())
    }

    fn read_timestamp<R: AsRef<[u8]>>(
        &self,
        column: &mut Vec<i64>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        let mut buffer_readr = Cursor::new(&buf);
        let ts = buffer_readr.read_timestamp_text(&self.common_settings().timezone, false)?;
        match ts {
            DateTimeResType::Datetime(ts) => {
                if !buffer_readr.eof() {
                    let data = buf.to_str().unwrap_or("not utf8");
                    let msg = format!(
                        "fail to deserialize timestamp, unexpected end at pos {} of {}",
                        buffer_readr.position(),
                        data
                    );
                    return Err(ErrorCode::BadBytes(msg));
                }
                let micros = ts.timestamp_micros();
                check_timestamp(micros)?;
                column.push(micros.as_());
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn read_array<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'[')?;
        for idx in 0.. {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b']') {
                break;
            }
            if idx != 0 {
                if let Err(err) = reader.must_ignore_byte(b',') {
                    self.pop_inner_values(&mut column.builder, idx);
                    return Err(err.into());
                }
            }
            let _ = reader.ignore_white_spaces();
            if let Err(err) = self.read_field(&mut column.builder, reader, positions) {
                self.pop_inner_values(&mut column.builder, idx);
                return Err(err);
            }
        }
        column.commit_row();
        Ok(())
    }

    fn read_map<R: AsRef<[u8]>>(
        &self,
        column: &mut ArrayColumnBuilder<AnyType>,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        const KEY: usize = 0;
        const VALUE: usize = 1;
        reader.must_ignore_byte(b'{')?;
        let mut set = HashSet::new();
        let map_builder = column.builder.as_tuple_mut().unwrap();
        for idx in 0.. {
            let _ = reader.ignore_white_spaces();
            if reader.ignore_byte(b'}') {
                break;
            }
            if idx != 0 {
                if let Err(err) = reader.must_ignore_byte(b',') {
                    self.pop_inner_values(&mut map_builder[KEY], idx);
                    self.pop_inner_values(&mut map_builder[VALUE], idx);
                    return Err(err.into());
                }
            }
            let _ = reader.ignore_white_spaces();
            if let Err(err) = self.read_field(&mut map_builder[KEY], reader, positions) {
                self.pop_inner_values(&mut map_builder[KEY], idx);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(err);
            }
            // check duplicate map keys
            let key = map_builder[KEY].pop().unwrap();
            if set.contains(&key) {
                self.pop_inner_values(&mut map_builder[KEY], idx);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(ErrorCode::BadBytes(
                    "map keys have to be unique".to_string(),
                ));
            }
            set.insert(key.clone());
            map_builder[KEY].push(key.as_ref());
            let _ = reader.ignore_white_spaces();
            if let Err(err) = reader.must_ignore_byte(b':') {
                self.pop_inner_values(&mut map_builder[KEY], idx + 1);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(err.into());
            }
            let _ = reader.ignore_white_spaces();
            if let Err(err) = self.read_field(&mut map_builder[VALUE], reader, positions) {
                self.pop_inner_values(&mut map_builder[KEY], idx + 1);
                self.pop_inner_values(&mut map_builder[VALUE], idx);
                return Err(err);
            }
        }
        column.commit_row();
        Ok(())
    }

    fn read_tuple<R: AsRef<[u8]>>(
        &self,
        fields: &mut [ColumnBuilder],
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        reader.must_ignore_byte(b'(')?;
        for idx in 0..fields.len() {
            let _ = reader.ignore_white_spaces();
            if idx != 0 {
                if let Err(err) = reader.must_ignore_byte(b',') {
                    for field in fields.iter_mut().take(idx) {
                        self.pop_inner_values(field, 1);
                    }
                    return Err(err.into());
                }
            }
            let _ = reader.ignore_white_spaces();
            if let Err(err) = self.read_field(&mut fields[idx], reader, positions) {
                for field in fields.iter_mut().take(idx) {
                    self.pop_inner_values(field, 1);
                }
                return Err(err);
            }
        }
        if let Err(err) = reader.must_ignore_byte(b')') {
            for field in fields.iter_mut() {
                self.pop_inner_values(field, 1);
            }
            return Err(err.into());
        }
        Ok(())
    }

    fn read_variant<R: AsRef<[u8]>>(
        &self,
        column: &mut StringColumnBuilder,
        reader: &mut Cursor<R>,
        positions: &mut VecDeque<usize>,
    ) -> Result<()> {
        let mut buf = Vec::new();
        self.read_string_inner(reader, &mut buf, positions)?;
        match parse_value(&buf) {
            Ok(value) => {
                value.write_to_vec(&mut column.data);
                column.commit_row();
            }
            Err(_) => {
                if self.common_settings().disable_variant_check {
                    column.put_slice(&buf);
                    column.commit_row();
                } else {
                    return Err(ErrorCode::BadBytes(format!(
                        "Invalid JSON value: {:?}",
                        String::from_utf8_lossy(&buf)
                    )));
                }
            }
        }
        Ok(())
    }
}

pub struct FastValuesDecoder<'a> {
    field_decoder: &'a FastFieldDecoderValues,
    reader: Cursor<&'a [u8]>,
    estimated_rows: usize,
    positions: VecDeque<usize>,
}

// Pre-generate the positions of `(`, `'` and `\`
static PATTERNS: &[&str] = &["(", "'", "\\"];

static INSERT_TOKEN_FINDER: Lazy<AhoCorasick> = Lazy::new(|| AhoCorasick::new(PATTERNS).unwrap());

impl<'a> FastValuesDecoder<'a> {
    pub fn new(data: &'a str, field_decoder: &'a FastFieldDecoderValues) -> Self {
        let mut estimated_rows = 0;
        let mut positions = VecDeque::new();
        for mat in INSERT_TOKEN_FINDER.find_iter(&data) {
            if mat.pattern() == 0.into() {
                estimated_rows += 1;
                continue;
            }
            positions.push_back(mat.start());
        }
        let reader = Cursor::new(data.as_bytes());
        FastValuesDecoder {
            reader,
            estimated_rows,
            positions,
            field_decoder,
        }
    }

    pub fn estimated_rows(&self) -> usize {
        self.estimated_rows
    }

    pub async fn parse<H, F>(
        &mut self,
        columns: &mut [ColumnBuilder],
        fallback_fn: &H,
    ) -> Result<()>
    where
        H: Fn(&str) -> F,
        F: std::future::Future<Output = Result<Vec<Scalar>>> + Send,
    {
        for row in 0.. {
            let _ = self.reader.ignore_white_spaces();
            if self.reader.eof() {
                break;
            }

            // Not the first row
            if row != 0 {
                if self.reader.ignore_byte(b';') {
                    break;
                }
                self.reader.must_ignore_byte(b',')?;
            }

            self.parse_next_row(columns, fallback_fn).await?;
        }
        Ok(())
    }

    async fn parse_next_row<H, F>(
        &mut self,
        columns: &mut [ColumnBuilder],
        fallback_fn: &H,
    ) -> Result<()>
    where
        H: Fn(&str) -> F,
        F: std::future::Future<Output = Result<Vec<Scalar>>> + Send,
    {
        let _ = self.reader.ignore_white_spaces();
        let col_size = columns.len();
        let start_pos_of_row = self.reader.checkpoint();

        // Start of the row --- '('
        if !self.reader.ignore_byte(b'(') {
            return Err(ErrorCode::BadDataValueType(
                "Must start with parentheses".to_string(),
            ));
        }
        // Ignore the positions in the previous row.
        while let Some(pos) = self.positions.front() {
            if *pos < start_pos_of_row as usize {
                self.positions.pop_front();
            } else {
                break;
            }
        }

        for col_idx in 0..col_size {
            let _ = self.reader.ignore_white_spaces();
            let col_end = if col_idx + 1 == col_size { b')' } else { b',' };

            let col = columns
                .get_mut(col_idx)
                .ok_or_else(|| ErrorCode::Internal("ColumnBuilder is None"))?;

            let (need_fallback, pop_count) = self
                .field_decoder
                .read_field(col, &mut self.reader, &mut self.positions)
                .map(|_| {
                    let _ = self.reader.ignore_white_spaces();
                    let need_fallback = self.reader.ignore_byte(col_end).not();
                    (need_fallback, col_idx + 1)
                })
                .unwrap_or((true, col_idx));

            // ColumnBuilder and expr-parser both will eat the end ')' of the row.
            if need_fallback {
                for col in columns.iter_mut().take(pop_count) {
                    col.pop();
                }
                // rollback to start position of the row
                self.reader.rollback(start_pos_of_row + 1);
                skip_to_next_row(&mut self.reader, 1)?;
                let end_pos_of_row = self.reader.position();

                // Parse from expression and append all columns.
                self.reader.set_position(start_pos_of_row);
                let row_len = end_pos_of_row - start_pos_of_row;
                let buf = &self.reader.remaining_slice()[..row_len as usize];

                let sql = std::str::from_utf8(buf).unwrap();
                let values = fallback_fn(sql).await?;

                for (col, scalar) in columns.iter_mut().zip(values) {
                    col.push(scalar.as_ref());
                }
                self.reader.set_position(end_pos_of_row);
                return Ok(());
            }
        }

        Ok(())
    }
}

// Values |(xxx), (yyy), (zzz)
pub fn skip_to_next_row<R: AsRef<[u8]>>(reader: &mut Cursor<R>, mut balance: i32) -> Result<()> {
    let _ = reader.ignore_white_spaces();

    let mut quoted = false;
    let mut escaped = false;

    while balance > 0 {
        let buffer = reader.remaining_slice();
        if buffer.is_empty() {
            break;
        }

        let size = buffer.len();

        let it = buffer
            .iter()
            .position(|&c| c == b'(' || c == b')' || c == b'\\' || c == b'\'');

        if let Some(it) = it {
            let c = buffer[it];
            reader.consume(it + 1);

            if it == 0 && escaped {
                escaped = false;
                continue;
            }
            escaped = false;

            match c {
                b'\\' => {
                    escaped = true;
                    continue;
                }
                b'\'' => {
                    quoted ^= true;
                    continue;
                }
                b')' => {
                    if !quoted {
                        balance -= 1;
                    }
                }
                b'(' => {
                    if !quoted {
                        balance += 1;
                    }
                }
                _ => {}
            }
        } else {
            escaped = false;
            reader.consume(size);
        }
    }
    Ok(())
}
