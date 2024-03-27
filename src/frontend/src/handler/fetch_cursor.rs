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

use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::{Format, Row};
use risingwave_sqlparser::ast::{FetchCursorStatement, Statement};

use super::query::handle_query;
use super::util::gen_query_from_logstore_ge_rw_timestamp;
use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};
use crate::session::cursor_manager::CursorRowValue;
use crate::{Binder, PgResponseStream};

pub async fn handle_fetch_cursor(
    handle_args: HandlerArgs,
    stmt: FetchCursorStatement,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();
    let db_name = session.database();
    let (schema_name, cursor_name) =
        Binder::resolve_schema_qualified_name(db_name, stmt.cursor_name.clone())?;

    let cursor_manager = session.get_cursor_manager();
    let mut cursor_manager = cursor_manager.lock().await;
    // Fetch data from the Cursor. There are three cases
    let (rw_timestamp, subscription_name, need_check_timestamp) = match cursor_manager
        .get_row_with_cursor(cursor_name.clone())
        .await?
    {
        CursorRowValue::Row((row, pg_descs)) => {
            // Normal row
            return Ok(build_fetch_cursor_response(vec![row], pg_descs));
        }
        CursorRowValue::QueryWithNextRwTimestamp(rw_timestamp, subscription_name) => {
            // Returned the rw_timestamp of the next cursor, query data and update the cursor
            (rw_timestamp, subscription_name, true)
        }
        CursorRowValue::QueryWithStartRwTimestamp(rw_timestamp, subscription_name) => {
            // The rw_timestamp for the next cursor has not been returned, and +1 query it.
            (rw_timestamp + 1, subscription_name, false)
        }
    };
    let subscription = session.get_subscription_by_name(
        schema_name,
        &subscription_name.0.last().unwrap().real_value().clone(),
    )?;
    let query_stmt = Statement::Query(Box::new(gen_query_from_logstore_ge_rw_timestamp(
        &subscription.get_log_store_name()?,
        rw_timestamp,
    )));
    let res = handle_query(handle_args, query_stmt, formats).await?;
    cursor_manager
        .update_cursor(
            cursor_name.clone(),
            res,
            rw_timestamp,
            false,
            need_check_timestamp,
            subscription_name.clone(),
            subscription.get_retention_seconds()?,
        )
        .await?;

    // Try fetch data after update cursor
    match cursor_manager.get_row_with_cursor(cursor_name).await? {
        CursorRowValue::Row((row, pg_descs)) => {
            Ok(build_fetch_cursor_response(vec![row], pg_descs))
        }
        CursorRowValue::QueryWithStartRwTimestamp(_, _) => {
            Ok(build_fetch_cursor_response(vec![], vec![]))
        }
        CursorRowValue::QueryWithNextRwTimestamp(_, _) => Err(ErrorCode::InternalError(
            "Fetch cursor, one must get a row or null".to_string(),
        )
        .into()),
    }
}

fn build_fetch_cursor_response(rows: Vec<Row>, pg_descs: Vec<PgFieldDescriptor>) -> RwPgResponse {
    PgResponse::builder(StatementType::FETCH)
        .row_cnt_opt(Some(rows.len() as i32))
        .values(PgResponseStream::from(rows), pg_descs)
        .into()
}
