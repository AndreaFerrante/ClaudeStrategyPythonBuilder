import polars as pl
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum
import time


# ============================================================================
# FOUNDATION CLASSES
# ============================================================================

class Severity(Enum):
    """Validation error severity levels"""
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ValidationIssue:
    """Single validation failure/warning"""
    validator_name: str
    severity: Severity
    rule_name: str
    description: str
    affected_rows: List[int] = field(default_factory=list)
    sample_data: Optional[Dict[str, Any]] = None
    
    def __str__(self) -> str:
        rows_info = f"({len(self.affected_rows)} rows)" if self.affected_rows else ""
        return f"[{self.severity.value}] {self.rule_name}: {self.description} {rows_info}"


@dataclass
class ValidationResult:
    """Result from a single validator"""
    validator_name: str
    passed: bool
    total_checks: int
    failed_checks: int
    issues: List[ValidationIssue] = field(default_factory=list)
    execution_time_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def pass_rate(self) -> float:
        if self.total_checks == 0:
            return 100.0
        return ((self.total_checks - self.failed_checks) / self.total_checks) * 100


class BaseValidator:
    """Abstract base class for all validators"""
    
    def __init__(self, name: str):
        self.name = name
        self.issues: List[ValidationIssue] = []
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        raise NotImplementedError("Subclasses must implement validate()")
    
    def add_issue(
        self, 
        severity: Severity,
        rule_name: str,
        description: str,
        affected_rows: Optional[List[int]] = None,
        sample_data: Optional[Dict[str, Any]] = None
    ) -> None:
        issue = ValidationIssue(
            validator_name=self.name,
            severity=severity,
            rule_name=rule_name,
            description=description,
            affected_rows=affected_rows or [],
            sample_data=sample_data
        )
        self.issues.append(issue)
    
    def _safe_select_debug_columns(self, df: pl.DataFrame, base_columns: List[str]) -> List[str]:
        """Return only columns that exist in the DataFrame"""
        available_cols = set(df.columns)
        return [col for col in base_columns if col in available_cols]
    
    def reset(self) -> None:
        self.issues = []


# ============================================================================
# VOLUME CONSERVATION VALIDATOR
# ============================================================================

class VolumeConservationValidator(BaseValidator):
    """
    Validates volume-related mathematical invariants:
    1. All volumes are non-negative
    2. Node_Volume tracks cumulative volume per price level correctly
    3. Session_Volume increments match tick volumes
    4. Session boundary volume resets are correct
    """
    
    def __init__(self):
        super().__init__("VolumeConservation")
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        self.reset()
        start_time = time.perf_counter()
        
        total_checks = 0
        failed_checks = 0
        
        result = self._check_non_negative_volumes(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_node_volume_per_price(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_session_volume_increments(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_session_boundary_volumes(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        execution_time = (time.perf_counter() - start_time) * 1000
        
        return ValidationResult(
            validator_name=self.name,
            passed=(failed_checks == 0),
            total_checks=total_checks,
            failed_checks=failed_checks,
            issues=self.issues.copy(),
            execution_time_ms=execution_time,
            metadata={
                'total_rows_analyzed': len(df),
                'checks_performed': 4
            }
        )
    
    def _check_non_negative_volumes(self, df: pl.DataFrame) -> Dict[str, int]:
        volume_cols = ['Volume', 'Node_Volume', 'Session_Volume']
        total = 0
        failed = 0
        
        for col in volume_cols:
            if col not in df.columns:
                continue
            total += 1
            df_indexed = df.with_row_index(name='original_index')
            negative_mask = df_indexed[col] < 0
            negative_count = negative_mask.sum()
            
            if negative_count > 0:
                failed += 1
                negative_rows = df_indexed.filter(negative_mask)
                debug_cols = self._safe_select_debug_columns(
                    negative_rows,
                    ['original_index', col, 'Price', 'Datetime', 'Date', 'Time', 'Sequence']
                )
                sample = negative_rows.head(10).select(debug_cols)
                self.add_issue(
                    severity=Severity.CRITICAL,
                    rule_name=f"NonNegative_{col}",
                    description=f"{col} contains {negative_count} negative values (IMPOSSIBLE)",
                    affected_rows=negative_rows['original_index'].to_list()[:100],
                    sample_data={
                        'min_value': float(df[col].min()),
                        'negative_count': int(negative_count),
                        'sample_violations': sample.to_dicts()
                    }
                )
        return {'total': total, 'failed': failed}
    
    def _check_node_volume_per_price(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Node_Volume' not in df.columns or 'Volume' not in df.columns or 'Price' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = df_check.with_columns([
            (pl.col('Session_Volume') < pl.col('Session_Volume').shift(1)).fill_null(False).alias('is_session_start')
        ])
        df_check = df_check.with_columns([
            pl.col('is_session_start').cum_sum().alias('session_id')
        ])
        df_check = df_check.with_columns([
            pl.col('Volume').cum_sum().over(['session_id', 'Price']).alias('expected_node_volume')
        ])
        
        tolerance = 0.001
        df_check = df_check.with_columns([
            (pl.col('Node_Volume') - pl.col('expected_node_volume')).abs().alias('node_volume_error')
        ])
        invalid_rows = df_check.filter(pl.col('node_volume_error') > tolerance)
        
        total = 1
        failed = 0
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence', 'Price',
                 'Volume', 'Node_Volume', 'expected_node_volume', 'node_volume_error', 'session_id']
            )
            sample_errors = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="NodeVolume_PerPriceAccumulation",
                description=f"Node_Volume doesn't track cumulative volume per price level correctly in {len(invalid_rows)} cases",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'max_error': float(invalid_rows['node_volume_error'].max()),
                    'mean_error': float(invalid_rows['node_volume_error'].mean()),
                    'sample_violations': sample_errors.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_session_volume_increments(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_Volume' not in df.columns or 'Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = df_check.with_columns([
            pl.col('Session_Volume').shift(1).alias('prev_session_volume'),
            (pl.col('Session_Volume') < pl.col('Session_Volume').shift(1)).fill_null(False).alias('is_session_start')
        ])
        df_check = df_check.filter(pl.col('prev_session_volume').is_not_null())
        df_check = df_check.with_columns([
            pl.when(pl.col('is_session_start'))
            .then(pl.col('Volume'))
            .otherwise(pl.col('prev_session_volume') + pl.col('Volume'))
            .alias('expected_session_volume')
        ])
        
        tolerance = 0.001
        df_check = df_check.with_columns([
            (pl.col('Session_Volume') - pl.col('expected_session_volume')).abs().alias('session_volume_error')
        ])
        invalid_rows = df_check.filter(pl.col('session_volume_error') > tolerance)
        
        total = 1
        failed = 0
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Volume', 'Session_Volume', 'expected_session_volume', 'session_volume_error', 'is_session_start']
            )
            sample_errors = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="SessionVolume_Increment",
                description=f"Session_Volume increments don't match tick Volume in {len(invalid_rows)} cases",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'max_error': float(invalid_rows['session_volume_error'].max()),
                    'sample_violations': sample_errors.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_session_boundary_volumes(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = df_check.with_columns([
            (pl.col('Session_Volume') < pl.col('Session_Volume').shift(1)).fill_null(False).alias('is_session_start')
        ])
        session_starts = df_check.filter(pl.col('is_session_start') == True)
        
        if len(session_starts) == 0:
            return {'total': 1, 'failed': 0}
        
        tolerance = 0.001
        invalid_starts = session_starts.filter(
            (pl.col('Session_Volume') - pl.col('Volume')).abs() > tolerance
        )
        
        total = 1
        failed = 0
        if len(invalid_starts) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_starts,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Volume', 'Session_Volume', 'Node_Volume']
            )
            sample = invalid_starts.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.WARNING,
                rule_name="SessionBoundary_VolumeReset",
                description=f"Session_Volume doesn't reset correctly at {len(invalid_starts)} session boundaries",
                affected_rows=invalid_starts['original_index'].to_list()[:50],
                sample_data={'sample_violations': sample.to_dicts()}
            )
        return {'total': total, 'failed': failed}


# ============================================================================
# SESSION HIGH/LOW VALIDATOR
# ============================================================================

class SessionHighLowValidator(BaseValidator):
    """
    Validates Session_High and Session_Low columns:
    1. Session_High >= Session_Low at all times
    2. Session_High is the running max of Price within a session
    3. Session_Low is the running min of Price within a session
    4. Session_High never decreases within a session
    5. Session_Low never increases within a session
    6. Current Price is always between Session_Low and Session_High (inclusive)
    7. Session_High and Session_Low reset correctly at session boundaries
    """
    
    def __init__(self):
        super().__init__("SessionHighLow")
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        self.reset()
        start_time = time.perf_counter()
        
        total_checks = 0
        failed_checks = 0
        
        result = self._check_high_gte_low(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_session_high_running_max(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_session_low_running_min(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_high_monotonic(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_low_monotonic(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_price_within_bounds(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_session_boundary_reset(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        execution_time = (time.perf_counter() - start_time) * 1000
        
        return ValidationResult(
            validator_name=self.name,
            passed=(failed_checks == 0),
            total_checks=total_checks,
            failed_checks=failed_checks,
            issues=self.issues.copy(),
            execution_time_ms=execution_time,
            metadata={
                'total_rows_analyzed': len(df),
                'checks_performed': 7
            }
        )
    
    def _add_session_id(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns([
            (pl.col('Session_Volume') < pl.col('Session_Volume').shift(1))
            .fill_null(False)
            .alias('is_session_start')
        ])
        df = df.with_columns([
            pl.col('is_session_start').cum_sum().alias('session_id')
        ])
        return df
    
    def _check_high_gte_low(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_High' not in df.columns or 'Session_Low' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        invalid_rows = df_check.filter(pl.col('Session_High') < pl.col('Session_Low'))
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'Session_High', 'Session_Low']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.CRITICAL,
                rule_name="High_GTE_Low",
                description=f"Session_High < Session_Low in {len(invalid_rows)} rows (IMPOSSIBLE)",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_session_high_running_max(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_High' not in df.columns or 'Price' not in df.columns or 'Session_Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)
        
        df_check = df_check.with_columns([
            pl.col('Price').cum_max().over('session_id').alias('expected_session_high')
        ])
        
        tolerance = 0.0001
        df_check = df_check.with_columns([
            (pl.col('Session_High') - pl.col('expected_session_high')).abs().alias('high_error')
        ])
        invalid_rows = df_check.filter(pl.col('high_error') > tolerance)
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'Session_High', 'expected_session_high', 'high_error', 'session_id']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="SessionHigh_RunningMax",
                description=f"Session_High doesn't match running max of Price in {len(invalid_rows)} rows",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'max_error': float(invalid_rows['high_error'].max()),
                    'mean_error': float(invalid_rows['high_error'].mean()),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_session_low_running_min(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_Low' not in df.columns or 'Price' not in df.columns or 'Session_Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)
        
        df_check = df_check.with_columns([
            pl.col('Price').cum_min().over('session_id').alias('expected_session_low')
        ])
        
        tolerance = 0.0001
        df_check = df_check.with_columns([
            (pl.col('Session_Low') - pl.col('expected_session_low')).abs().alias('low_error')
        ])
        invalid_rows = df_check.filter(pl.col('low_error') > tolerance)
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'Session_Low', 'expected_session_low', 'low_error', 'session_id']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="SessionLow_RunningMin",
                description=f"Session_Low doesn't match running min of Price in {len(invalid_rows)} rows",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'max_error': float(invalid_rows['low_error'].max()),
                    'mean_error': float(invalid_rows['low_error'].mean()),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_high_monotonic(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_High' not in df.columns or 'Session_Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)
        
        df_check = df_check.with_columns([
            pl.col('Session_High').shift(1).over('session_id').alias('prev_session_high'),
            pl.col('session_id').shift(1).alias('prev_session_id')
        ])
        
        invalid_rows = df_check.filter(
            (pl.col('prev_session_high').is_not_null()) &
            (pl.col('prev_session_id') == pl.col('session_id')) &
            (pl.col('Session_High') < pl.col('prev_session_high') - 0.0001)
        )
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'Session_High', 'prev_session_high', 'session_id']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.CRITICAL,
                rule_name="SessionHigh_Monotonic",
                description=f"Session_High decreased within a session in {len(invalid_rows)} rows (IMPOSSIBLE)",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_low_monotonic(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_Low' not in df.columns or 'Session_Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)
        
        df_check = df_check.with_columns([
            pl.col('Session_Low').shift(1).over('session_id').alias('prev_session_low'),
            pl.col('session_id').shift(1).alias('prev_session_id')
        ])
        
        invalid_rows = df_check.filter(
            (pl.col('prev_session_low').is_not_null()) &
            (pl.col('prev_session_id') == pl.col('session_id')) &
            (pl.col('Session_Low') > pl.col('prev_session_low') + 0.0001)
        )
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'Session_Low', 'prev_session_low', 'session_id']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.CRITICAL,
                rule_name="SessionLow_Monotonic",
                description=f"Session_Low increased within a session in {len(invalid_rows)} rows (IMPOSSIBLE)",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_price_within_bounds(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_High' not in df.columns or 'Session_Low' not in df.columns or 'Price' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        
        tolerance = 0.0001
        invalid_rows = df_check.filter(
            (pl.col('Price') > pl.col('Session_High') + tolerance) |
            (pl.col('Price') < pl.col('Session_Low') - tolerance)
        )
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            above_high = invalid_rows.filter(pl.col('Price') > pl.col('Session_High') + tolerance)
            below_low = invalid_rows.filter(pl.col('Price') < pl.col('Session_Low') - tolerance)
            
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'Session_High', 'Session_Low']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.CRITICAL,
                rule_name="Price_WithinSessionBounds",
                description=(
                    f"Price outside Session_High/Session_Low bounds in {len(invalid_rows)} rows "
                    f"({len(above_high)} above high, {len(below_low)} below low) (IMPOSSIBLE)"
                ),
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'above_high_count': len(above_high),
                    'below_low_count': len(below_low),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_session_boundary_reset(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'Session_High' not in df.columns or 'Session_Low' not in df.columns or 'Session_Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = df_check.with_columns([
            (pl.col('Session_Volume') < pl.col('Session_Volume').shift(1))
            .fill_null(False)
            .alias('is_session_start')
        ])
        
        session_starts = df_check.filter(pl.col('is_session_start') == True)
        
        if len(session_starts) == 0:
            return {'total': 1, 'failed': 0}
        
        tolerance = 0.0001
        invalid_starts = session_starts.filter(
            ((pl.col('Session_High') - pl.col('Price')).abs() > tolerance) |
            ((pl.col('Session_Low') - pl.col('Price')).abs() > tolerance)
        )
        
        total = 1
        failed = 0
        
        if len(invalid_starts) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_starts,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'Session_High', 'Session_Low']
            )
            sample = invalid_starts.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.WARNING,
                rule_name="SessionBoundary_HighLowReset",
                description=(
                    f"Session_High and/or Session_Low don't equal Price at "
                    f"{len(invalid_starts)} session boundaries"
                ),
                affected_rows=invalid_starts['original_index'].to_list()[:50],
                sample_data={
                    'total_session_starts': len(session_starts),
                    'invalid_starts': len(invalid_starts),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}


# ============================================================================
# POC VALIDATOR
# ============================================================================

class POCValidator(BaseValidator):
    """
    Validates the Point of Control (POC) column:
    1. POC is always between Session_Low and Session_High
    2. POC aligns to valid price tick increments
    3. POC is the price with the highest Node_Volume in the session so far
    4. POC only changes when a new price overtakes the current highest-volume node
    """
    
    def __init__(self, tick_size: float = 0.25):
        super().__init__("POC")
        self.tick_size = tick_size
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        self.reset()
        start_time = time.perf_counter()
        
        total_checks = 0
        failed_checks = 0
        
        result = self._check_poc_within_session_bounds(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_poc_tick_alignment(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_poc_is_highest_volume(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        result = self._check_poc_change_justified(df)
        total_checks += result['total']
        failed_checks += result['failed']
        
        execution_time = (time.perf_counter() - start_time) * 1000
        
        return ValidationResult(
            validator_name=self.name,
            passed=(failed_checks == 0),
            total_checks=total_checks,
            failed_checks=failed_checks,
            issues=self.issues.copy(),
            execution_time_ms=execution_time,
            metadata={
                'total_rows_analyzed': len(df),
                'checks_performed': 4,
                'tick_size': self.tick_size
            }
        )
    
    def _add_session_id(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns([
            (pl.col('Session_Volume') < pl.col('Session_Volume').shift(1))
            .fill_null(False)
            .alias('is_session_start')
        ])
        df = df.with_columns([
            pl.col('is_session_start').cum_sum().alias('session_id')
        ])
        return df
    
    def _check_poc_within_session_bounds(self, df: pl.DataFrame) -> Dict[str, int]:
        required = ['POC', 'Session_High', 'Session_Low']
        if not all(col in df.columns for col in required):
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        
        tolerance = 0.0001
        invalid_rows = df_check.filter(
            (pl.col('POC') > pl.col('Session_High') + tolerance) |
            (pl.col('POC') < pl.col('Session_Low') - tolerance)
        )
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            above = invalid_rows.filter(pl.col('POC') > pl.col('Session_High') + tolerance)
            below = invalid_rows.filter(pl.col('POC') < pl.col('Session_Low') - tolerance)
            
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'POC', 'Session_High', 'Session_Low']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.CRITICAL,
                rule_name="POC_WithinSessionBounds",
                description=(
                    f"POC outside Session_High/Session_Low in {len(invalid_rows)} rows "
                    f"({len(above)} above high, {len(below)} below low)"
                ),
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'above_high_count': len(above),
                    'below_low_count': len(below),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_poc_tick_alignment(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'POC' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        
        multiplier = round(1.0 / self.tick_size)
        df_check = df_check.with_columns([
            ((pl.col('POC') * multiplier) % 1.0).abs().alias('tick_remainder')
        ])
        
        tolerance = 0.001
        invalid_rows = df_check.filter(
            (pl.col('tick_remainder') > tolerance) &
            (pl.col('tick_remainder') < 1.0 - tolerance)
        )
        
        total = 1
        failed = 0
        
        if len(invalid_rows) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'POC', 'tick_remainder']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="POC_TickAlignment",
                description=f"POC not aligned to tick size {self.tick_size} in {len(invalid_rows)} rows",
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'tick_size': self.tick_size,
                    'invalid_count': len(invalid_rows),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_poc_is_highest_volume(self, df: pl.DataFrame) -> Dict[str, int]:
        required = ['POC', 'Price', 'Volume', 'Node_Volume', 'Session_Volume']
        if not all(col in df.columns for col in required):
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)
        
        df_check = df_check.with_columns([
            pl.col('Volume')
            .cum_sum()
            .over(['session_id', 'Price'])
            .alias('cum_vol_at_price')
        ])
        
        df_check = df_check.with_columns([
            pl.col('Node_Volume').cum_max().over('session_id').alias('max_node_volume_in_session')
        ])
        
        poc_rows = df_check.filter(pl.col('Price') == pl.col('POC'))
        
        tolerance = 0.001
        poc_mismatch = poc_rows.filter(
            (pl.col('Node_Volume') - pl.col('max_node_volume_in_session')).abs() > tolerance
        )
        
        non_poc_rows = df_check.filter(pl.col('Price') != pl.col('POC'))
        non_poc_violations = non_poc_rows.filter(
            pl.col('Node_Volume') > pl.col('max_node_volume_in_session') + tolerance
        )
        
        total = 1
        failed = 0
        
        all_violations_count = len(poc_mismatch) + len(non_poc_violations)
        
        if all_violations_count > 0:
            failed = 1
            
            description_parts = []
            sample_rows = pl.DataFrame()
            
            if len(poc_mismatch) > 0:
                description_parts.append(
                    f"POC price doesn't have highest Node_Volume in {len(poc_mismatch)} rows where Price==POC"
                )
                debug_cols = self._safe_select_debug_columns(
                    poc_mismatch,
                    ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                     'Price', 'POC', 'Node_Volume', 'max_node_volume_in_session', 'session_id']
                )
                sample_rows = poc_mismatch.head(10).select(debug_cols)
            
            if len(non_poc_violations) > 0:
                description_parts.append(
                    f"Non-POC price has Node_Volume > session max in {len(non_poc_violations)} rows"
                )
                if len(sample_rows) == 0:
                    debug_cols = self._safe_select_debug_columns(
                        non_poc_violations,
                        ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                         'Price', 'POC', 'Node_Volume', 'max_node_volume_in_session', 'session_id']
                    )
                    sample_rows = non_poc_violations.head(10).select(debug_cols)
            
            affected = (
                poc_mismatch['original_index'].to_list()[:50] + 
                non_poc_violations['original_index'].to_list()[:50]
            )
            
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="POC_HighestVolume",
                description="; ".join(description_parts),
                affected_rows=affected,
                sample_data={
                    'poc_mismatch_count': len(poc_mismatch),
                    'non_poc_violation_count': len(non_poc_violations),
                    'sample_violations': sample_rows.to_dicts() if len(sample_rows) > 0 else []
                }
            )
        return {'total': total, 'failed': failed}
    
    def _check_poc_change_justified(self, df: pl.DataFrame) -> Dict[str, int]:
        if 'POC' not in df.columns or 'Session_Volume' not in df.columns:
            return {'total': 0, 'failed': 0}
        
        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)
        
        df_check = df_check.with_columns([
            pl.col('POC').shift(1).alias('prev_poc'),
            pl.col('session_id').shift(1).alias('prev_session_id')
        ])
        
        poc_changes = df_check.filter(
            (pl.col('prev_poc').is_not_null()) &
            (pl.col('prev_session_id') == pl.col('session_id')) &
            (pl.col('POC') != pl.col('prev_poc'))
        )
        
        if len(poc_changes) == 0:
            return {'total': 1, 'failed': 0}
        
        unjustified_changes = poc_changes.filter(
            pl.col('POC') != pl.col('Price')
        )
        
        total = 1
        failed = 0
        
        if len(unjustified_changes) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                unjustified_changes,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'POC', 'prev_poc', 'Node_Volume', 'Volume', 'session_id']
            )
            sample = unjustified_changes.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.WARNING,
                rule_name="POC_ChangeJustified",
                description=(
                    f"POC changed to a price different from the current tick's Price "
                    f"in {len(unjustified_changes)} of {len(poc_changes)} POC changes"
                ),
                affected_rows=unjustified_changes['original_index'].to_list()[:100],
                sample_data={
                    'total_poc_changes': len(poc_changes),
                    'unjustified_changes': len(unjustified_changes),
                    'justified_pct': round(
                        (1 - len(unjustified_changes) / len(poc_changes)) * 100, 2
                    ),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}


# ============================================================================
# VALUE AREA VALIDATOR
# ============================================================================

class ValueAreaValidator(BaseValidator):
    """
    Validates the VA_areas column which labels each tick as:
      - "PO"  : tick Price is at the POC
      - "VA"  : tick Price is within the Value Area (68% volume band around POC)
      - "na"  : tick Price is outside the Value Area

    Checks performed:
    1. Only valid labels exist (PO, VA, na)
    2. PO label consistency: VA_areas == "PO" if and only if Price == POC
    3. PO label exists in every session (at least one tick must hit the POC)
    4. VA symmetry: VA prices are contiguous around the POC (no gaps)
    5. VA volume coverage: the volume within PO + VA rows approximates 68%
       of total session volume at that point in time
    6. Monotonic expansion: the VA boundary prices can only widen or stay
       the same within a session, never shrink
    7. NA labels are outside VA boundaries: any "na" tick's Price must be
       above the VA high boundary or below the VA low boundary
    """

    def __init__(self, va_percentage: float = 0.68, tolerance_pct: float = 0.05):
        super().__init__("ValueArea")
        self.va_percentage = va_percentage
        self.tolerance_pct = tolerance_pct  # allowed deviation from 68%

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        self.reset()
        start_time = time.perf_counter()

        total_checks = 0
        failed_checks = 0

        # Check 1: Valid labels only
        result = self._check_valid_labels(df)
        total_checks += result['total']
        failed_checks += result['failed']

        # Check 2: PO ↔ Price == POC consistency
        result = self._check_po_label_consistency(df)
        total_checks += result['total']
        failed_checks += result['failed']

        # Check 3: Every session has at least one PO label
        result = self._check_po_exists_per_session(df)
        total_checks += result['total']
        failed_checks += result['failed']

        # Check 4: VA prices are contiguous around POC (no gaps)
        result = self._check_va_contiguous(df)
        total_checks += result['total']
        failed_checks += result['failed']

        # Check 5: VA volume coverage ≈ 68%
        result = self._check_va_volume_coverage(df)
        total_checks += result['total']
        failed_checks += result['failed']

        # Check 6: VA boundaries only widen within a session
        result = self._check_va_monotonic_expansion(df)
        total_checks += result['total']
        failed_checks += result['failed']

        # Check 7: NA prices are outside VA boundaries
        result = self._check_na_outside_va(df)
        total_checks += result['total']
        failed_checks += result['failed']

        execution_time = (time.perf_counter() - start_time) * 1000

        return ValidationResult(
            validator_name=self.name,
            passed=(failed_checks == 0),
            total_checks=total_checks,
            failed_checks=failed_checks,
            issues=self.issues.copy(),
            execution_time_ms=execution_time,
            metadata={
                'total_rows_analyzed': len(df),
                'checks_performed': 7,
                'va_percentage': self.va_percentage,
                'tolerance_pct': self.tolerance_pct
            }
        )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _add_session_id(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns([
            (pl.col('Session_Volume') < pl.col('Session_Volume').shift(1))
            .fill_null(False)
            .alias('is_session_start')
        ])
        df = df.with_columns([
            pl.col('is_session_start').cum_sum().alias('session_id')
        ])
        return df

    # ------------------------------------------------------------------
    # Check 1
    # ------------------------------------------------------------------
    def _check_valid_labels(self, df: pl.DataFrame) -> Dict[str, int]:
        """Rule: VA_areas must contain only 'PO', 'VA', or 'na'"""
        if 'VA_areas' not in df.columns:
            return {'total': 0, 'failed': 0}

        valid_labels = {"PO", "VA", "na"}
        df_check = df.with_row_index(name='original_index')
        invalid_rows = df_check.filter(~pl.col('VA_areas').is_in(valid_labels))

        total = 1
        failed = 0

        if len(invalid_rows) > 0:
            failed = 1
            unique_bad = invalid_rows['VA_areas'].unique().to_list()
            debug_cols = self._safe_select_debug_columns(
                invalid_rows,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'POC', 'VA_areas']
            )
            sample = invalid_rows.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.CRITICAL,
                rule_name="VA_ValidLabels",
                description=(
                    f"VA_areas contains {len(invalid_rows)} rows with invalid labels: "
                    f"{unique_bad}"
                ),
                affected_rows=invalid_rows['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_rows),
                    'invalid_labels': unique_bad,
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}

    # ------------------------------------------------------------------
    # Check 2
    # ------------------------------------------------------------------
    def _check_po_label_consistency(self, df: pl.DataFrame) -> Dict[str, int]:
        """
        Rule: VA_areas == 'PO'  ↔  Price == POC
        - If Price == POC the label MUST be 'PO'
        - If Price != POC the label must NOT be 'PO'
        """
        required = ['VA_areas', 'Price', 'POC']
        if not all(c in df.columns for c in required):
            return {'total': 0, 'failed': 0}

        tolerance = 0.0001
        df_check = df.with_row_index(name='original_index')
        df_check = df_check.with_columns([
            ((pl.col('Price') - pl.col('POC')).abs() < tolerance).alias('price_is_poc')
        ])

        # Case A: Price == POC but label is not PO
        missing_po = df_check.filter(
            (pl.col('price_is_poc')) & (pl.col('VA_areas') != 'PO')
        )

        # Case B: Price != POC but label is PO
        false_po = df_check.filter(
            (~pl.col('price_is_poc')) & (pl.col('VA_areas') == 'PO')
        )

        total = 1
        failed = 0
        all_bad = len(missing_po) + len(false_po)

        if all_bad > 0:
            failed = 1
            parts = []
            sample_rows = pl.DataFrame()

            if len(missing_po) > 0:
                parts.append(
                    f"Price==POC but label is not 'PO' in {len(missing_po)} rows"
                )
                debug_cols = self._safe_select_debug_columns(
                    missing_po,
                    ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                     'Price', 'POC', 'VA_areas']
                )
                sample_rows = missing_po.head(10).select(debug_cols)

            if len(false_po) > 0:
                parts.append(
                    f"Price!=POC but label is 'PO' in {len(false_po)} rows"
                )
                if len(sample_rows) == 0:
                    debug_cols = self._safe_select_debug_columns(
                        false_po,
                        ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                         'Price', 'POC', 'VA_areas']
                    )
                    sample_rows = false_po.head(10).select(debug_cols)

            affected = (
                missing_po['original_index'].to_list()[:50] +
                false_po['original_index'].to_list()[:50]
            )

            self.add_issue(
                severity=Severity.ERROR,
                rule_name="PO_LabelConsistency",
                description="; ".join(parts),
                affected_rows=affected,
                sample_data={
                    'missing_po_count': len(missing_po),
                    'false_po_count': len(false_po),
                    'sample_violations': sample_rows.to_dicts() if len(sample_rows) > 0 else []
                }
            )
        return {'total': total, 'failed': failed}

    # ------------------------------------------------------------------
    # Check 3
    # ------------------------------------------------------------------
    def _check_po_exists_per_session(self, df: pl.DataFrame) -> Dict[str, int]:
        """Rule: Every session should contain at least one row with VA_areas == 'PO'"""
        required = ['VA_areas', 'Session_Volume']
        if not all(c in df.columns for c in required):
            return {'total': 0, 'failed': 0}

        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)

        po_counts = (
            df_check
            .group_by('session_id')
            .agg([
                (pl.col('VA_areas') == 'PO').sum().alias('po_count'),
                pl.col('original_index').first().alias('session_first_row')
            ])
        )

        sessions_without_po = po_counts.filter(pl.col('po_count') == 0)

        total = 1
        failed = 0

        if len(sessions_without_po) > 0:
            failed = 1
            self.add_issue(
                severity=Severity.WARNING,
                rule_name="PO_ExistsPerSession",
                description=(
                    f"{len(sessions_without_po)} sessions have zero rows labelled 'PO' "
                    f"(no tick hit the POC price)"
                ),
                affected_rows=sessions_without_po['session_first_row'].to_list()[:50],
                sample_data={
                    'total_sessions': len(po_counts),
                    'sessions_without_po': len(sessions_without_po),
                    'missing_session_ids': sessions_without_po['session_id'].to_list()[:20]
                }
            )
        return {'total': total, 'failed': failed}

    # ------------------------------------------------------------------
    # Check 4
    # ------------------------------------------------------------------
    def _check_va_contiguous(self, df: pl.DataFrame) -> Dict[str, int]:
        """
        Rule: Within each session the set of prices labelled PO or VA must
        form a contiguous range (no gaps). If price 6050 and 6051 are VA
        then 6050.50 cannot be 'na' if it was traded.

        We check: collect all unique VA/PO prices per session, sort them,
        and verify consecutive prices differ by at most one tick size.
        We use the minimum price step observed in the data as tick size.
        """
        required = ['VA_areas', 'Price', 'Session_Volume']
        if not all(c in df.columns for c in required):
            return {'total': 0, 'failed': 0}

        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)

        # Determine tick size from data
        unique_prices = df_check['Price'].unique().sort()
        if len(unique_prices) < 2:
            return {'total': 1, 'failed': 0}

        price_diffs = unique_prices.diff().drop_nulls().filter(
            pl.col('Price') > 0.0
        )
        if len(price_diffs) == 0:
            return {'total': 1, 'failed': 0}
        tick_size = float(price_diffs.min())

        # For each session get the sorted unique prices with VA or PO label
        va_po_prices = (
            df_check
            .filter(pl.col('VA_areas').is_in(['PO', 'VA']))
            .group_by('session_id')
            .agg(pl.col('Price').unique().sort().alias('va_prices'))
        )

        gap_sessions = []
        gap_details = []
        tolerance = tick_size * 1.5  # allow a tiny float margin

        for row in va_po_prices.iter_rows(named=True):
            prices = row['va_prices']
            if len(prices) < 2:
                continue
            for i in range(1, len(prices)):
                diff = prices[i] - prices[i - 1]
                if diff > tolerance:
                    gap_sessions.append(row['session_id'])
                    gap_details.append({
                        'session_id': row['session_id'],
                        'price_below_gap': prices[i - 1],
                        'price_above_gap': prices[i],
                        'gap_size': round(diff, 4),
                        'expected_max_step': tick_size
                    })
                    break  # one example per session is enough

        total = 1
        failed = 0

        if len(gap_sessions) > 0:
            failed = 1
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="VA_Contiguous",
                description=(
                    f"VA/PO price range has gaps in {len(gap_sessions)} sessions "
                    f"(VA prices are not contiguous around POC)"
                ),
                affected_rows=[],
                sample_data={
                    'sessions_with_gaps': len(gap_sessions),
                    'tick_size_detected': tick_size,
                    'sample_violations': gap_details[:10]
                }
            )
        return {'total': total, 'failed': failed}

    # ------------------------------------------------------------------
    # Check 5
    # ------------------------------------------------------------------
    def _check_va_volume_coverage(self, df: pl.DataFrame) -> Dict[str, int]:
        """
        Rule: At the end of each session the cumulative volume of prices
        labelled PO or VA should approximate va_percentage (68%) of the
        total session volume.

        We allow ± tolerance_pct deviation.
        """
        required = ['VA_areas', 'Volume', 'Session_Volume']
        if not all(c in df.columns for c in required):
            return {'total': 0, 'failed': 0}

        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)

        session_stats = (
            df_check
            .group_by('session_id')
            .agg([
                pl.col('Volume').sum().alias('total_volume'),
                pl.col('Volume')
                  .filter(pl.col('VA_areas').is_in(['PO', 'VA']))
                  .sum()
                  .alias('va_volume'),
                pl.col('original_index').last().alias('session_last_row')
            ])
        )

        session_stats = session_stats.with_columns([
            (pl.col('va_volume') / pl.col('total_volume')).alias('va_ratio')
        ])

        low_bound = self.va_percentage - self.tolerance_pct
        high_bound = self.va_percentage + self.tolerance_pct

        outlier_sessions = session_stats.filter(
            (pl.col('va_ratio') < low_bound) | (pl.col('va_ratio') > high_bound)
        )

        total = 1
        failed = 0

        if len(outlier_sessions) > 0:
            failed = 1
            sample = outlier_sessions.head(10).to_dicts()
            self.add_issue(
                severity=Severity.WARNING,
                rule_name="VA_VolumeCoverage",
                description=(
                    f"VA+PO volume ratio outside "
                    f"[{low_bound:.0%}, {high_bound:.0%}] "
                    f"in {len(outlier_sessions)} of {len(session_stats)} sessions"
                ),
                affected_rows=outlier_sessions['session_last_row'].to_list()[:50],
                sample_data={
                    'expected_range': f"[{low_bound:.2%}, {high_bound:.2%}]",
                    'sessions_out_of_range': len(outlier_sessions),
                    'total_sessions': len(session_stats),
                    'sample_violations': sample
                }
            )
        return {'total': total, 'failed': failed}

    # ------------------------------------------------------------------
    # Check 6
    # ------------------------------------------------------------------
    def _check_va_monotonic_expansion(self, df: pl.DataFrame) -> Dict[str, int]:
        """
        Rule: The VA boundaries (highest and lowest VA/PO price seen so far
        in the session) can only widen or stay the same, never shrink.

        We track the running VA_high and VA_low per session and verify
        they are monotonically non-decreasing / non-increasing respectively.
        """
        required = ['VA_areas', 'Price', 'Session_Volume']
        if not all(c in df.columns for c in required):
            return {'total': 0, 'failed': 0}

        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)

        # Assign VA/PO price or null for rows outside VA
        df_check = df_check.with_columns([
            pl.when(pl.col('VA_areas').is_in(['PO', 'VA']))
            .then(pl.col('Price'))
            .otherwise(pl.lit(None))
            .alias('va_price')
        ])

        # Running VA boundaries using forward fill after cum_max / cum_min
        # over only the non-null va_price values
        df_check = df_check.with_columns([
            pl.col('va_price').cum_max().over('session_id').alias('va_high_running'),
            pl.col('va_price').cum_min().over('session_id').alias('va_low_running')
        ])

        # Check VA high never decreases
        df_check = df_check.with_columns([
            pl.col('va_high_running').shift(1).over('session_id').alias('prev_va_high'),
            pl.col('va_low_running').shift(1).over('session_id').alias('prev_va_low'),
            pl.col('session_id').shift(1).alias('prev_session_id')
        ])

        tolerance = 0.0001

        high_shrink = df_check.filter(
            (pl.col('prev_va_high').is_not_null()) &
            (pl.col('prev_session_id') == pl.col('session_id')) &
            (pl.col('va_high_running') < pl.col('prev_va_high') - tolerance)
        )

        low_shrink = df_check.filter(
            (pl.col('prev_va_low').is_not_null()) &
            (pl.col('prev_session_id') == pl.col('session_id')) &
            (pl.col('va_low_running') > pl.col('prev_va_low') + tolerance)
        )

        total = 1
        failed = 0
        all_bad = len(high_shrink) + len(low_shrink)

        if all_bad > 0:
            failed = 1
            parts = []
            sample_rows = pl.DataFrame()

            if len(high_shrink) > 0:
                parts.append(
                    f"VA upper boundary shrank in {len(high_shrink)} rows"
                )
                debug_cols = self._safe_select_debug_columns(
                    high_shrink,
                    ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                     'Price', 'VA_areas', 'va_high_running', 'prev_va_high', 'session_id']
                )
                sample_rows = high_shrink.head(10).select(debug_cols)

            if len(low_shrink) > 0:
                parts.append(
                    f"VA lower boundary shrank in {len(low_shrink)} rows"
                )
                if len(sample_rows) == 0:
                    debug_cols = self._safe_select_debug_columns(
                        low_shrink,
                        ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                         'Price', 'VA_areas', 'va_low_running', 'prev_va_low', 'session_id']
                    )
                    sample_rows = low_shrink.head(10).select(debug_cols)

            affected = (
                high_shrink['original_index'].to_list()[:50] +
                low_shrink['original_index'].to_list()[:50]
            )

            self.add_issue(
                severity=Severity.ERROR,
                rule_name="VA_MonotonicExpansion",
                description="; ".join(parts),
                affected_rows=affected,
                sample_data={
                    'high_shrink_count': len(high_shrink),
                    'low_shrink_count': len(low_shrink),
                    'sample_violations': sample_rows.to_dicts() if len(sample_rows) > 0 else []
                }
            )
        return {'total': total, 'failed': failed}

    # ------------------------------------------------------------------
    # Check 7
    # ------------------------------------------------------------------
    def _check_na_outside_va(self, df: pl.DataFrame) -> Dict[str, int]:
        """
        Rule: Any tick labelled 'na' should have its Price strictly outside
        the current VA boundaries (the running VA high / VA low at that
        point in the session).

        If Price is between VA low and VA high (inclusive) the label should
        be 'VA' or 'PO', not 'na'.
        """
        required = ['VA_areas', 'Price', 'Session_Volume']
        if not all(c in df.columns for c in required):
            return {'total': 0, 'failed': 0}

        df_check = df.with_row_index(name='original_index')
        df_check = self._add_session_id(df_check)

        # Build running VA boundaries from VA/PO prices
        df_check = df_check.with_columns([
            pl.when(pl.col('VA_areas').is_in(['PO', 'VA']))
            .then(pl.col('Price'))
            .otherwise(pl.lit(None))
            .alias('va_price')
        ])

        df_check = df_check.with_columns([
            pl.col('va_price').cum_max().over('session_id').alias('va_high_running'),
            pl.col('va_price').cum_min().over('session_id').alias('va_low_running')
        ])

        tolerance = 0.0001

        # 'na' rows whose Price is within the running VA boundaries
        invalid_na = df_check.filter(
            (pl.col('VA_areas') == 'na') &
            (pl.col('va_high_running').is_not_null()) &
            (pl.col('va_low_running').is_not_null()) &
            (pl.col('Price') >= pl.col('va_low_running') - tolerance) &
            (pl.col('Price') <= pl.col('va_high_running') + tolerance)
        )

        total = 1
        failed = 0

        if len(invalid_na) > 0:
            failed = 1
            debug_cols = self._safe_select_debug_columns(
                invalid_na,
                ['original_index', 'Datetime', 'Date', 'Time', 'Sequence',
                 'Price', 'POC', 'VA_areas', 'va_high_running', 'va_low_running',
                 'session_id']
            )
            sample = invalid_na.head(10).select(debug_cols)
            self.add_issue(
                severity=Severity.ERROR,
                rule_name="NA_OutsideVA",
                description=(
                    f"{len(invalid_na)} rows labelled 'na' have Price inside "
                    f"the current VA boundaries (should be 'VA' or 'PO')"
                ),
                affected_rows=invalid_na['original_index'].to_list()[:100],
                sample_data={
                    'invalid_count': len(invalid_na),
                    'sample_violations': sample.to_dicts()
                }
            )
        return {'total': total, 'failed': failed}


# ============================================================================
# VALIDATION RUNNER
# ============================================================================

class ValidationRunner:
    """Orchestrates multiple validators and produces a combined report"""

    def __init__(self):
        self.validators: List[BaseValidator] = []
        self.results: List[ValidationResult] = []

    def add_validator(self, validator: BaseValidator) -> 'ValidationRunner':
        self.validators.append(validator)
        return self

    def run_all(self, df: pl.DataFrame) -> List[ValidationResult]:
        self.results = []
        for validator in self.validators:
            result = validator.validate(df)
            self.results.append(result)
        return self.results

    def print_report(self) -> None:
        total_checks = sum(r.total_checks for r in self.results)
        total_failed = sum(r.failed_checks for r in self.results)
        all_passed = all(r.passed for r in self.results)
        total_time = sum(r.execution_time_ms for r in self.results)

        print("\n" + "=" * 90)
        print("COMPREHENSIVE VALIDATION REPORT")
        print("=" * 90)
        print(f"Overall Status:    {'✅ ALL PASSED' if all_passed else '❌ FAILURES DETECTED'}")
        print(f"Validators Run:    {len(self.results)}")
        print(f"Total Checks:      {total_checks}")
        print(f"Total Failed:      {total_failed}")
        print(f"Overall Pass Rate: {((total_checks - total_failed) / total_checks * 100) if total_checks > 0 else 100:.2f}%")
        print(f"Total Time:        {total_time:.2f}ms ({total_time / 1000:.2f}s)")

        for result in self.results:
            print("\n" + "-" * 90)
            status = "✅ PASSED" if result.passed else "❌ FAILED"
            print(f"Validator: {result.validator_name} | {status} | "
                  f"{result.pass_rate:.1f}% pass | "
                  f"{result.total_checks} checks | "
                  f"{result.execution_time_ms:.1f}ms")

            if result.issues:
                for i, issue in enumerate(result.issues, 1):
                    print(f"\n  {i}. {issue}")
                    if issue.sample_data:
                        for key, value in issue.sample_data.items():
                            if key == 'sample_violations' and isinstance(value, list):
                                print(f"\n     🔍 {key.upper()} (showing first {len(value)}):")
                                for idx, violation in enumerate(value, 1):
                                    print(f"\n        Row {idx}:")
                                    for k, v in violation.items():
                                        print(f"           {k:30s}: {v}")
                            else:
                                print(f"        {key:25s}: {value}")

        print("\n" + "=" * 90)
        if all_passed:
            print("✅ All validations passed - data is mathematically consistent!")
        else:
            critical = sum(
                1 for r in self.results for iss in r.issues if iss.severity == Severity.CRITICAL
            )
            errors = sum(
                1 for r in self.results for iss in r.issues if iss.severity == Severity.ERROR
            )
            warnings = sum(
                1 for r in self.results for iss in r.issues if iss.severity == Severity.WARNING
            )
            print(f"Issues summary: {critical} CRITICAL | {errors} ERRORS | {warnings} WARNINGS")
        print("=" * 90)


# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("✅ All classes loaded successfully")

# Load the CSV data
df_ticks = pl.read_csv(
    r'C:\Users\Tommy\Documents\PycharmProjects\Orderflow\Sources\ES\ESZ25-CME_ENR_20251212_225959.csv',
    separator=';'
)

df_ticks = df_ticks.with_columns(
    pl.col("Datetime")
    .str.strptime(
        pl.Datetime,
        "%Y-%m-%dT%H:%M:%S%.f",
        strict=True
    )
    .alias("Datetime")
)

print(f"✅ Data loaded: {len(df_ticks):,} rows")
print(f"   Columns: {df_ticks.columns}")

# Build and run all validators
runner = ValidationRunner()
runner.add_validator(VolumeConservationValidator())
runner.add_validator(SessionHighLowValidator())
runner.add_validator(POCValidator(tick_size=0.25))
runner.add_validator(ValueAreaValidator(va_percentage=0.68, tolerance_pct=0.05))

runner.run_all(df_ticks)
runner.print_report()