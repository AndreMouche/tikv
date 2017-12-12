// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use tipb::schema::ColumnInfo;
use tipb::select::{Chunk, DAGRequest, SelectResponse};
use kvproto::coprocessor::{KeyRange, Response};
use protobuf::{Message as PbMsg, RepeatedField};

use coprocessor::codec::mysql;
use coprocessor::codec::datum::{Datum, DatumEncoder};
use coprocessor::select::xeval::EvalContext;
use coprocessor::{Error, Result};
use coprocessor::cache::*;
use coprocessor::metrics::*;
use coprocessor::endpoint::{get_pk, to_pb_error, ReqContext};
use storage::{Snapshot, SnapshotStore, Statistics};

use super::executor::{build_exec, Executor, Row};

pub struct DAGContext {
    columns: Arc<Vec<ColumnInfo>>,
    has_aggr: bool,
    has_topn: bool,
    req_ctx: Arc<ReqContext>,
    exec: Box<Executor>,
    output_offsets: Vec<u32>,
    batch_row_limit: usize,
    cache_key: String,
    enable_distsql_cache: bool,
}

impl DAGContext {
    pub fn new(
        mut req: DAGRequest,
        ranges: Vec<KeyRange>,
        snap: Box<Snapshot>,
        req_ctx: Arc<ReqContext>,
        batch_row_limit: usize,
        enable_distsql_cache: bool,
    ) -> Result<DAGContext> {
        let eval_ctx = Arc::new(box_try!(EvalContext::new(
            req.get_time_zone_offset(),
            req.get_flags()
        )));
        let store = SnapshotStore::new(
            snap,
            req.get_start_ts(),
            req_ctx.isolation_level,
            req_ctx.fill_cache,
        );
        let cache_key = format!("{:?}, {:?}", ranges, req.get_executors());

        let dag_executor = build_exec(req.take_executors().into_vec(), store, ranges, eval_ctx)?;
        Ok(DAGContext {
            columns: dag_executor.columns,
            has_aggr: dag_executor.has_aggr,
            has_topn: dag_executor.has_topn,
            req_ctx: req_ctx,
            exec: dag_executor.exec,
            output_offsets: req.take_output_offsets(),
            batch_row_limit: batch_row_limit,
            cache_key: cache_key,
            enable_distsql_cache: enable_distsql_cache,
        })
    }

    pub fn handle_request(&mut self, region_id: u64) -> Result<Response> {
        let mut record_cnt = 0;
        let mut chunks = Vec::new();
        let mut version: u64 = 0;
        if self.can_cache() {
            version = DISTSQL_CACHE.lock().unwrap().get_region_version(region_id);
            if let Some(data) = DISTSQL_CACHE
                .lock()
                .unwrap()
                .get(region_id, &self.cache_key)
            {
                debug!("Cache Hit: {}, region_id: {}", self.cache_key, region_id);
                CORP_DISTSQL_CACHE_COUNT.with_label_values(&["hit"]).inc();
                let mut resp = Response::new();
                resp.set_data(data.clone());
                return Ok(resp);
            };
        }

        loop {
            match self.exec.next() {
                Ok(Some(row)) => {
                    self.req_ctx.check_if_outdated()?;
                    if chunks.is_empty() || record_cnt >= self.batch_row_limit {
                        let chunk = Chunk::new();
                        chunks.push(chunk);
                        record_cnt = 0;
                    }
                    let chunk = chunks.last_mut().unwrap();
                    record_cnt += 1;
                    if self.has_aggr {
                        chunk.mut_rows_data().extend_from_slice(&row.data.value);
                    } else {
                        let value = inflate_cols(&row, &self.columns, &self.output_offsets)?;
                        chunk.mut_rows_data().extend_from_slice(&value);
                    }
                }
                Ok(None) => {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_chunks(RepeatedField::from_vec(chunks));
                    let data = box_try!(sel_resp.write_to_bytes());
                    // If result data is greater than 5MB should not cache it.
                    if self.can_cache_with_size(&data) {
                        debug!("Cache It: {}, region_id: {}", &self.cache_key, region_id);
                        DISTSQL_CACHE
                            .lock()
                            .unwrap()
                            .put(region_id, self.cache_key.clone(), version, data.clone());
                        CORP_DISTSQL_CACHE_COUNT.with_label_values(&["miss"]).inc();
                    }
                    resp.set_data(data);
                    return Ok(resp);
                }
                Err(e) => if let Error::Other(_) = e {
                    let mut resp = Response::new();
                    let mut sel_resp = SelectResponse::new();
                    sel_resp.set_error(to_pb_error(&e));
                    resp.set_data(box_try!(sel_resp.write_to_bytes()));
                    resp.set_other_error(format!("{}", e));
                    return Ok(resp);
                } else {
                    return Err(e);
                },
            }
        }
    }

    pub fn collect_statistics_into(&mut self, statistics: &mut Statistics) {
        self.exec.collect_statistics_into(statistics);
    }

    pub fn can_cache(&mut self) -> bool {
        self.enable_distsql_cache && (self.has_aggr || self.has_topn)
    }

    pub fn can_cache_with_size(&mut self, data: &[u8]) -> bool {
        if data.len() > 5 * 1024 * 1024 {
            false
        } else {
            self.can_cache()
        }
    }
}

#[inline]
fn inflate_cols(row: &Row, cols: &[ColumnInfo], output_offsets: &[u32]) -> Result<Vec<u8>> {
    let data = &row.data;
    // TODO capacity is not enough
    let mut values = Vec::with_capacity(data.value.len());
    for offset in output_offsets {
        let col = &cols[*offset as usize];
        let col_id = col.get_column_id();
        match data.get(col_id) {
            Some(value) => values.extend_from_slice(value),
            None if col.get_pk_handle() => {
                let pk = get_pk(col, row.handle);
                box_try!(values.encode(&[pk], false));
            }
            None if col.has_default_val() => {
                values.extend_from_slice(col.get_default_val());
            }
            None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                return Err(box_err!("column {} of {} is missing", col_id, row.handle));
            }
            None => {
                box_try!(values.encode(&[Datum::Null], false));
            }
        }
    }
    Ok(values)
}
