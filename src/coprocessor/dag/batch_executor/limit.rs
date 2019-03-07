// Copyright 2019 PingCAP, Inc.
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

use tipb::expression::Expr;
use super::interface::*;
use crate::coprocessor::dag::executor::ExprColumnRefVisitor;
use crate::coprocessor::dag::rpn_expr::{RpnExpressionNodeVec, RpnRuntimeContext};
use crate::coprocessor::Result;

pub struct BatchLimitExecutor<Src:BatchExecutor> {
    context:BatchExecutorContext,
    src:Src,
    rt_context:RpnRuntimeContext,
    expect_rows:u64,
}

impl<Src:BatchExecutor> BatchLimitExecutor<Src> {
    pub fn new(context:BatchExecutorContext,src:Src,limit:u64)->Result<Self> {
        let rt_context = RpnRuntimeContext::new(context.config);
        Ok(Self{
            context,
            src,
            rt_context,
            expect_rows:limit
        })
    }
}

impl<Src:BatchExecutor> BatchExecutor for BatchLimitExecutor<Src> {
    #[inline]
    fn next_batch(&mut self,expect_rows:usize)->BatchExecuteResult {
        if expect_rows > self.expect_rows {
            expect_rows = self.expect_rows;
        }
        // we needn't check whether expect_rows is 0 since src will check it.
        let result = self.src.next_batch(expect_rows);
        let rows_len = result.data.rows_len();
        self.expect_rows -= rows_len;
        result
    }
    
    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
    }
}