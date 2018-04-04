// Copyright 2018 PingCAP, Inc.
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

use std::{mem, i64, u64};
use std::sync::Arc;

use chrono::FixedOffset;
use tipb::select;
use super::{Error, Result};

/// Flags are used by `DAGRequest.flags` to handle execution mode, like how to handle
/// truncate error.
/// `FLAG_IGNORE_TRUNCATE` indicates if truncate error should be ignored.
/// Read-only statements should ignore truncate error, write statements should not ignore
/// truncate error.
pub const FLAG_IGNORE_TRUNCATE: u64 = 1;
/// `FLAG_TRUNCATE_AS_WARNING` indicates if truncate error should be returned as warning.
/// This flag only matters if `FLAG_IGNORE_TRUNCATE` is not set, in strict sql mode, truncate error
/// should be returned as error, in non-strict sql mode, truncate error should be saved as warning.
pub const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;

// `FLAG_IN_SELECT_STMT` indicates if this is a SELECT statement.
pub const FLAG_IN_SELECT_STMT: u64 = 1 << 5;
/// `FLAG_OVERFLOW_AS_WARNING` indicates if overflow error should be returned as warning.
/// In strict sql mode, overflow error should be returned as error,
/// in non-strict sql mode, overflow error should be saved as warning.
pub const FLAG_OVERFLOW_AS_WARNING: u64 = 1 << 6;

const DEFAULT_MAX_WARNING_CNT: usize = 64;
#[derive(Debug)]
pub struct EvalConfig {
    /// timezone to use when parse/calculate time.
    pub tz: FixedOffset,
    pub ignore_truncate: bool,
    pub truncate_as_warning: bool,
    pub overflow_as_warning: bool,
    pub in_select_stmt: bool,
    pub max_warning_cnt: usize,
}

impl Default for EvalConfig {
    fn default() -> EvalConfig {
        EvalConfig {
            tz: FixedOffset::east(0),
            ignore_truncate: false,
            truncate_as_warning: false,
            overflow_as_warning: false,
            in_select_stmt: false,
            max_warning_cnt: DEFAULT_MAX_WARNING_CNT,
        }
    }
}

impl EvalConfig {
    pub fn new(tz_offset: i64, flags: u64) -> Result<EvalConfig> {
        if tz_offset <= -ONE_DAY || tz_offset >= ONE_DAY {
            return Err(Error::gen_unknown_timezone(tz_offset));
        }
        let tz = match FixedOffset::east_opt(tz_offset as i32) {
            None => return Err(Error::gen_unknown_timezone(tz_offset)),
            Some(tz) => tz,
        };

        let e = EvalConfig {
            tz: tz,
            ignore_truncate: (flags & FLAG_IGNORE_TRUNCATE) > 0,
            truncate_as_warning: (flags & FLAG_TRUNCATE_AS_WARNING) > 0,
            overflow_as_warning: (flags & FLAG_OVERFLOW_AS_WARNING) > 0,
            in_select_stmt: (flags & FLAG_IN_SELECT_STMT) > 0,
            max_warning_cnt: DEFAULT_MAX_WARNING_CNT,
        };

        Ok(e)
    }

    pub fn set_max_warning_cnt(&mut self, max_warning_cnt: usize) {
        self.max_warning_cnt = max_warning_cnt;
    }

    pub fn new_eval_warnings(&self) -> EvalWarnings {
        EvalWarnings::new(self.max_warning_cnt)
    }
}

// Warning details caused in eval computation.
#[derive(Debug, Default)]
pub struct EvalWarnings {
    // max number of warnings to return.
    max_warning_cnt: usize,
    // number of warnings
    pub warning_cnt: usize,
    // details of previous max_warning_cnt warnings
    pub warnings: Vec<select::Error>,
}

impl EvalWarnings {
    fn new(max_warning_cnt: usize) -> EvalWarnings {
        EvalWarnings {
            max_warning_cnt: max_warning_cnt,
            warning_cnt: 0,
            warnings: Vec::with_capacity(max_warning_cnt),
        }
    }

    pub fn append_warning(&mut self, err: Error) {
        self.warning_cnt += 1;
        if self.warnings.len() < self.max_warning_cnt {
            self.warnings.push(err.into());
        }
    }

    pub fn merge(&mut self, mut other: EvalWarnings) {
        self.warning_cnt += other.warning_cnt;
        if self.warnings.len() >= self.max_warning_cnt {
            return;
        }
        other
            .warnings
            .truncate(self.max_warning_cnt - self.warnings.len());
        self.warnings.append(&mut other.warnings);
    }
}

#[derive(Debug)]
/// Some global variables needed in an evaluation.
pub struct EvalContext {
    pub cfg: Arc<EvalConfig>,
    pub warnings: EvalWarnings,
}

impl Default for EvalContext {
    fn default() -> EvalContext {
        let cfg = Arc::new(EvalConfig::default());
        let warnings = cfg.new_eval_warnings();
        EvalContext {
            cfg: cfg,
            warnings: warnings,
        }
    }
}
const ONE_DAY: i64 = 3600 * 24;

impl EvalContext {
    pub fn new(cfg: Arc<EvalConfig>) -> EvalContext {
        let warnings = cfg.new_eval_warnings();
        EvalContext {
            cfg: cfg,
            warnings: warnings,
        }
    }

    pub fn handle_truncate(&mut self, is_truncated: bool) -> Result<()> {
        if !is_truncated {
            return Ok(());
        }
        self.handle_truncate_err(Error::Truncated("[1265] Data Truncated".into()))
    }

    pub fn handle_truncate_err(&mut self, err: Error) -> Result<()> {
        if self.cfg.ignore_truncate {
            return Ok(());
        }
        if self.cfg.truncate_as_warning {
            self.warnings.append_warning(err);
            return Ok(());
        }
        Err(err)
    }

    /// handle_over_flow treats ErrOverflow as warnings or returns the error
    /// based on the cfg.handle_over_flow state.
    pub fn handle_over_flow(&mut self, err: Error) -> Result<()> {
        if self.cfg.overflow_as_warning {
            self.warnings.append_warning(err);
            Ok(())
        } else {
            Err(err)
        }
    }

    pub fn overflow_from_cast_str_as_int(
        &mut self,
        bytes: &[u8],
        orig_err: Error,
        negitive: bool,
    ) -> Result<i64> {
        if !self.cfg.in_select_stmt | !self.cfg.overflow_as_warning {
            return Err(orig_err);
        }
        let orig_str = String::from_utf8_lossy(bytes).into_owned();
        self.warnings
            .append_warning(Error::gen_truncated_wrong_val("INTEGER", orig_str));
        if negitive {
            Ok(i64::MIN)
        } else {
            Ok(u64::MAX as i64)
        }
    }

    pub fn take_warnings(&mut self) -> EvalWarnings {
        mem::replace(
            &mut self.warnings,
            EvalWarnings::new(self.cfg.max_warning_cnt),
        )
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use super::*;

    #[test]
    fn test_handle_truncate() {
        // ignore_truncate = false, truncate_as_warning = false
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, 0).unwrap()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_err());
        assert!(ctx.take_warnings().warnings.is_empty());
        // ignore_truncate = false;
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.take_warnings().warnings.is_empty());

        // ignore_truncate = false, truncate_as_warning = true
        let mut ctx = EvalContext::new(Arc::new(
            EvalConfig::new(0, FLAG_TRUNCATE_AS_WARNING).unwrap(),
        ));
        assert!(ctx.handle_truncate(false).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(!ctx.take_warnings().warnings.is_empty());
    }

    #[test]
    fn test_max_warning_cnt() {
        let eval_cfg = Arc::new(EvalConfig::new(0, FLAG_TRUNCATE_AS_WARNING).unwrap());
        let mut ctx = EvalContext::new(Arc::clone(&eval_cfg));
        assert!(ctx.handle_truncate(true).is_ok());
        assert!(ctx.handle_truncate(true).is_ok());
        assert_eq!(ctx.take_warnings().warnings.len(), 2);
        for _ in 0..2 * DEFAULT_MAX_WARNING_CNT {
            assert!(ctx.handle_truncate(true).is_ok());
        }
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.warning_cnt, 2 * DEFAULT_MAX_WARNING_CNT);
        assert_eq!(warnings.warnings.len(), eval_cfg.max_warning_cnt);
    }
}
