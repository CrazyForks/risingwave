// Copyright 2024 RisingWave Labs
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

use core::ops::Index;
use core::time::Duration;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::types::Row;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::ObjectName;

use crate::error::{ErrorCode, Result};
use crate::handler::util::convert_logstore_i64_to_epoch;
use crate::handler::RwPgResponse;
pub struct Cursor {
    cursor_name: String,
    rw_pg_response: RwPgResponse,
    data_chunk_cache: VecDeque<Row>,
    rw_timestamp: i64,
    is_snapshot: bool,
    subscription_name: ObjectName,
    pg_desc: Vec<PgFieldDescriptor>,
}

impl Cursor {
    pub async fn new(
        cursor_name: String,
        mut rw_pg_response: RwPgResponse,
        start_timestamp: i64,
        is_snapshot: bool,
        need_check_timestamp: bool,
        subscription_name: ObjectName,
    ) -> Result<Self> {
        let (rw_timestamp, data_chunk_cache) = if is_snapshot {
            // Cursor created based on table, no need to update start_timestamp
            (start_timestamp, vec![])
        } else {
            let data_chunk_cache = rw_pg_response
                .values_stream()
                .next()
                .await
                .unwrap_or_else(|| Ok(Vec::new()))
                .map_err(|e| {
                    ErrorCode::InternalError(format!(
                        "Cursor get next chunk error {:?}",
                        e.to_string()
                    ))
                })?;
            // Use the first line of the log store to update start_timestamp
            let query_timestamp = data_chunk_cache
                .get(0)
                .map(|row| {
                    row.index(0)
                        .as_ref()
                        .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
                        .unwrap()
                })
                .unwrap_or_else(|| start_timestamp);
            if need_check_timestamp
                && (data_chunk_cache.is_empty() || query_timestamp != start_timestamp)
            {
                // If the previous cursor returns next_rw_timestamp, then this check is triggered,
                // and query_timestamp and start_timestamp must be equal to each other to prevent data errors caused by two long cursor times
                return Err(ErrorCode::InternalError(format!(
                    " No data found for rw_timestamp {:?}, data may have been recycled, please recreate cursor"
                ,convert_logstore_i64_to_epoch(start_timestamp))).into());
            }
            (query_timestamp, data_chunk_cache)
        };
        let pg_desc = build_desc(rw_pg_response.row_desc(), is_snapshot);
        Ok(Self {
            cursor_name,
            rw_pg_response,
            data_chunk_cache: VecDeque::from(data_chunk_cache),
            rw_timestamp,
            is_snapshot,
            subscription_name,
            pg_desc,
        })
    }

    pub async fn next(&mut self) -> Result<CursorRowValue> {
        let stream = self.rw_pg_response.values_stream();
        loop {
            if self.data_chunk_cache.is_empty() {
                // 1. Cache is empty, need to query data
                if let Some(row_set) = stream.next().await {
                    // 1a. Get the data from the stream and consume it in the next cycle
                    self.data_chunk_cache = VecDeque::from(row_set.map_err(|e| {
                        ErrorCode::InternalError(format!(
                            "Cursor get next chunk error {:?}",
                            e.to_string()
                        ))
                    })?);
                } else {
                    // 1b. No data was fetched and next_rw_timestamp was not found, so need to query using the rw_timestamp+1.
                    return Ok(CursorRowValue::QueryWithStartRwTimestamp(
                        self.rw_timestamp,
                        self.subscription_name.clone(),
                    ));
                }
            }
            if let Some(row) = self.data_chunk_cache.pop_front() {
                // 2. fetch data
                let new_row = row.take();
                if self.is_snapshot {
                    // 2a. The rw_timestamp in the table is all the same, so don't need to check.
                    return Ok(CursorRowValue::Row((
                        Row::new(build_row_with_snapshot(new_row, self.rw_timestamp)),
                        self.pg_desc.clone(),
                    )));
                }

                let timestamp_row: i64 = new_row
                    .get(0)
                    .unwrap()
                    .as_ref()
                    .map(|bytes| std::str::from_utf8(bytes).unwrap().parse().unwrap())
                    .unwrap();

                if timestamp_row != self.rw_timestamp {
                    // 2b. Find next_rw_timestamp, need update cursor with next_rw_timestamp.
                    return Ok(CursorRowValue::QueryWithNextRwTimestamp(
                        timestamp_row,
                        self.subscription_name.clone(),
                    ));
                } else {
                    // 2c. The rw_timestamp of this row is equal to self.rw_timestamp, return row
                    return Ok(CursorRowValue::Row((
                        Row::new(build_row_with_logstore(new_row, timestamp_row)?),
                        self.pg_desc.clone(),
                    )));
                }
            }
        }
    }
}

pub fn build_row_with_snapshot(row: Vec<Option<Bytes>>, rw_timestamp: i64) -> Vec<Option<Bytes>> {
    let mut new_row = vec![
        Some(Bytes::from(
            convert_logstore_i64_to_epoch(rw_timestamp).to_string(),
        )),
        Some(Bytes::from(1i16.to_string())),
    ];
    new_row.extend(row);
    new_row
}

pub fn build_row_with_logstore(
    mut row: Vec<Option<Bytes>>,
    rw_timestamp: i64,
) -> Result<Vec<Option<Bytes>>> {
    // remove sqr_id, vnode ,_row_id
    let mut new_row = vec![Some(Bytes::from(
        convert_logstore_i64_to_epoch(rw_timestamp).to_string(),
    ))];
    new_row.extend(row.drain(3..row.len() - 1).collect_vec());
    Ok(new_row)
}

pub fn build_desc(mut descs: Vec<PgFieldDescriptor>, is_snapshot: bool) -> Vec<PgFieldDescriptor> {
    let mut new_descs = vec![
        PgFieldDescriptor::new(
            "rw_timestamp".to_owned(),
            DataType::Varchar.to_oid(),
            DataType::Varchar.type_len(),
        ),
        PgFieldDescriptor::new(
            "op".to_owned(),
            DataType::Int16.to_oid(),
            DataType::Int16.type_len(),
        ),
    ];
    if is_snapshot {
        new_descs.extend(descs)
    } else {
        new_descs.extend(descs.drain(4..descs.len() - 1));
    }
    new_descs
}

pub enum CursorRowValue {
    Row((Row, Vec<PgFieldDescriptor>)),
    QueryWithNextRwTimestamp(i64, ObjectName),
    QueryWithStartRwTimestamp(i64, ObjectName),
}
#[derive(Default)]
pub struct CursorManager {
    cursor_map: HashMap<String, (Cursor, Instant)>,
    // Save subsciption's retentain_secs
    cursor_retention_secs_maps: HashMap<ObjectName, Duration>,
}

impl CursorManager {
    pub fn add_cursor_retention_secs(
        &mut self,
        subscription_name: ObjectName,
        retention_secs: Duration,
    ) {
        self.cursor_retention_secs_maps
            .insert(subscription_name, retention_secs);
    }

    pub fn add_cursor(&mut self, cursor: Cursor) -> Result<()> {
        let cursor_need_drop_time = Instant::now()
            + *self
                .cursor_retention_secs_maps
                .get(&cursor.subscription_name)
                .ok_or_else(|| {
                    ErrorCode::InternalError(format!(
                        "Cursor can't find retention time for subscription_name: {:?}",
                        cursor.subscription_name
                    ))
                })?;
        self.cursor_map
            .insert(cursor.cursor_name.clone(), (cursor, cursor_need_drop_time));
        Ok(())
    }

    pub fn update_cursor(&mut self, cursor: Cursor) -> Result<()> {
        let cursor_need_drop_time = Instant::now()
            + *self
                .cursor_retention_secs_maps
                .get(&cursor.subscription_name)
                .ok_or_else(|| {
                    ErrorCode::InternalError(format!(
                        "Cursor can't find retention time for subscription_name: {:?}",
                        cursor.subscription_name
                    ))
                })?;
        self.cursor_map
            .insert(cursor.cursor_name.clone(), (cursor, cursor_need_drop_time));
        Ok(())
    }

    pub fn remove_cursor(&mut self, cursor_name: String) -> Result<()> {
        self.cursor_map.remove(&cursor_name);
        Ok(())
    }

    pub async fn get_row_with_cursor(&mut self, cursor_name: String) -> Result<CursorRowValue> {
        if let Some((cursor, cursor_need_drop_time)) = self.cursor_map.get_mut(&cursor_name) {
            if Instant::now() > *cursor_need_drop_time {
                self.remove_cursor(cursor_name)?;
                return Err(ErrorCode::InternalError(
                    "The cursor has exceeded its maximum lifetime, please recreate it.".to_string(),
                )
                .into());
            }
            cursor.next().await
        } else {
            Err(ErrorCode::ItemNotFound(format!("Don't find cursor `{}`", cursor_name)).into())
        }
    }
}
