from typing import Dict, List, Tuple, Optional
from datetime import datetime, date, time as dt_time, timedelta
import polars as pl
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import time


# ============================================================================
# FOUNDATION CLASSES
# ============================================================================

class Severity(Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ValidationIssue:
    validator_name: str
    severity: Severity
    rule_name: str
    description: str
    affected_rows: List[int] = field(default_factory=list)
    sample_data: Optional[Dict] = None
    
    def __str__(self) -> str:
        rows_info = f"({len(self.affected_rows)} rows)" if self.affected_rows else ""
        return f"[{self.severity.value}] {self.rule_name}: {self.description} {rows_info}"


@dataclass
class ValidationResult:
    validator_name: str
    passed: bool
    total_checks: int
    failed_checks: int
    issues: List[ValidationIssue] = field(default_factory=list)
    execution_time_ms: float = 0.0
    metadata: Dict = field(default_factory=dict)
    
    @property
    def pass_rate(self) -> float:
        if self.total_checks == 0:
            return 100.0
        return ((self.total_checks - self.failed_checks) / self.total_checks) * 100


class BaseValidator:
    def __init__(self, name: str):
        self.name = name
        self.issues: List[ValidationIssue] = []
    
    def add_issue(
        self, 
        severity: Severity,
        rule_name: str,
        description: str,
        affected_rows: Optional[List[int]] = None,
        sample_data: Optional[Dict] = None
    ) -> None:
        self.issues.append(ValidationIssue(
            validator_name=self.name,
            severity=severity,
            rule_name=rule_name,
            description=description,
            affected_rows=affected_rows or [],
            sample_data=sample_data
        ))
    
    def reset(self) -> None:
        self.issues = []


# ============================================================================
# VP SESSION DEFINITIONS
# ============================================================================

class VPSession:
    """
    Volume Profile Session:
    - ETH: 16:00 → 08:29:59.999999 (next day)
    - RTH: 08:30 → 15:59:59.999999
    - VP: ETH start (16:00) → RTH end (15:59:59 next day)
    """
    
    ETH_START_HOUR = 16  # 4 PM
    RTH_START_HOUR = 8
    RTH_START_MINUTE = 30
    RTH_END_HOUR = 15
    RTH_END_MINUTE = 59
    
    @staticmethod
    def get_vp_session_id(dt: datetime) -> date:
        """Get VP session date (date of ETH start at 16:00)"""
        hour = dt.hour
        minute = dt.minute
        
        # VP session runs 16:00 → 15:59:59 next day
        if hour >= VPSession.ETH_START_HOUR:
            # After 16:00 → today's session
            return dt.date()
        else:
            # Before 16:00 → yesterday's session
            return (dt - timedelta(days=1)).date()
    
    @staticmethod
    def get_vp_session_bounds(session_date: date) -> Tuple[datetime, datetime]:
        """Get VP session start and end datetimes"""
        start_dt = datetime.combine(session_date, dt_time(hour=16, minute=0))
        end_dt = datetime.combine(
            session_date + timedelta(days=1), 
            dt_time(hour=15, minute=59, second=59, microsecond=999999)
        )
        return start_dt, end_dt


# ============================================================================
# SIERRA CHART LOADER
# ============================================================================

class SierraChartDataLoader:
    """Loads Sierra Chart 1-minute bar data"""
    
    COLUMN_NAMES = [
        'Date', 'Time',
        'Open', 'High', 'Low', 'Last', 'Volume', 'NumTrades',
        'OHLC_Avg', 'HLC_Avg', 'HL_Avg', 'BidVolume', 'AskVolume',
        'CD_Open', 'CD_High', 'CD_Low', 'CD_Close',  # Ignore these
        'CD_Volume', 'CD_OHLC_Avg', 'CD_HLC_Avg', 'CD_HL_Avg',  # Ignore these
        'POC', 'VAH', 'VAL', 'VWAP'
    ]
    
    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
    
    def load(self) -> pl.DataFrame:
        """Load and parse Sierra CSV"""
        if not self.csv_path.exists():
            raise FileNotFoundError(f"Sierra CSV not found: {self.csv_path}")
        
        print(f"   Loading Sierra CSV...")
        
        df = pl.read_csv(
            self.csv_path,
            has_header=True,
            separator=',',
            try_parse_dates=False,
            infer_schema_length=0
        )
        
        df.columns = self.COLUMN_NAMES
        
        # Cast to correct types
        df = df.with_columns([
            pl.col('Open').str.strip_chars().cast(pl.Float64, strict=False),
            pl.col('High').str.strip_chars().cast(pl.Float64, strict=False),
            pl.col('Low').str.strip_chars().cast(pl.Float64, strict=False),
            pl.col('Last').str.strip_chars().cast(pl.Float64, strict=False),
            pl.col('Volume').str.strip_chars().cast(pl.Int64, strict=False),
            pl.col('POC').str.strip_chars().cast(pl.Float64, strict=False),
            pl.col('VAH').str.strip_chars().cast(pl.Float64, strict=False),
            pl.col('VAL').str.strip_chars().cast(pl.Float64, strict=False),
        ])
        
        # Parse datetime
        df = df.with_columns([
            (pl.col('Date').str.strip_chars() + ' ' + pl.col('Time').str.strip_chars()).alias('datetime_str')
        ])
        
        df = df.with_columns([
            pl.col('datetime_str')
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=False)
            .alias('Datetime')
        ])
        
        # Add VP session ID
        df = df.with_columns([
            pl.col('Datetime').map_elements(
                lambda dt: VPSession.get_vp_session_id(dt),
                return_dtype=pl.Date
            ).alias('VP_SessionDate')
        ])
        
        print(f"   ✓ Loaded {len(df)} bars")
        return df


# ============================================================================
# BAR VALIDATION METRICS
# ============================================================================

@dataclass
class BarValidationResult:
    """Result for a single bar comparison"""
    bar_datetime: datetime
    
    # POC
    poc_match: bool
    python_poc: float
    sierra_poc: float
    
    # VA_Areas
    va_label_match: bool
    python_label: str
    expected_label: str
    tick_price: float
    
    # Session Bounds
    session_high_match: bool
    session_low_match: bool
    python_high: float
    python_low: float
    calculated_high: float
    calculated_low: float
    
    # OHLC
    ohlc_match: bool
    python_ohlc: Dict[str, float]
    sierra_ohlc: Dict[str, float]
    
    # Last tick index for export
    last_tick_index: int


@dataclass
class SessionValidationMetrics:
    """Validation metrics for entire session"""
    session_date: date
    total_bars: int
    
    # POC metrics
    poc_correct_bars: int
    poc_accuracy: float
    poc_passed: bool
    
    # VA_Areas metrics
    va_correct_bars: int
    va_accuracy: float
    va_passed: bool
    
    # Session bounds metrics
    session_bounds_correct_bars: int
    session_bounds_accuracy: float
    session_bounds_passed: bool
    
    # OHLC metrics
    ohlc_correct_bars: int
    ohlc_accuracy: float
    ohlc_passed: bool
    
    # Failed bars for export
    failed_bar_results: List[BarValidationResult]
    
    @property
    def all_passed(self) -> bool:
        return all([
            self.poc_passed,
            self.va_passed,
            self.session_bounds_passed,
            self.ohlc_passed
        ])


# ============================================================================
# L1 VALIDATOR - BAR BY BAR
# ============================================================================

class SierraChartValidator(BaseValidator):
    """
    L1 Validation: Bar-by-bar comparison with configurable tolerances
    
    For each Sierra bar:
    - Find last Python tick in that bar's time window
    - Calculate session metrics up to that bar
    - Compare tick values vs Sierra bar values
    
    Tolerances are % of bars that must pass (not price differences)
    """
    
    def __init__(
        self,
        tick_size: float = 0.25,
        poc_tolerance_pct: float = 0.0,      # 0% = 100% accuracy required
        va_tolerance_pct: float = 0.05,      # 5% = 95% accuracy required
        session_bounds_tolerance_pct: float = 0.05,
        ohlc_tolerance_pct: float = 0.05
    ):
        super().__init__("SierraChartReference")
        self.tick_size = tick_size
        self.poc_tolerance_pct = poc_tolerance_pct
        self.va_tolerance_pct = va_tolerance_pct
        self.session_bounds_tolerance_pct = session_bounds_tolerance_pct
        self.ohlc_tolerance_pct = ohlc_tolerance_pct
        
        self.session_metrics: List[SessionValidationMetrics] = []
        self.processed_python_df: Optional[pl.DataFrame] = None
    
    def validate(
        self,
        python_df: pl.DataFrame,
        sierra_csv_path: str
    ) -> ValidationResult:
        """Main validation entry point"""
        self.reset()
        start_time = time.perf_counter()
        
        print("\n" + "="*80)
        print("L1 SIERRA CHART VALIDATION - BAR-BY-BAR")
        print("="*80)
        print(f"Tolerances: POC={self.poc_tolerance_pct*100:.0f}%, "
              f"VA={self.va_tolerance_pct*100:.0f}%, "
              f"Bounds={self.session_bounds_tolerance_pct*100:.0f}%, "
              f"OHLC={self.ohlc_tolerance_pct*100:.0f}%")
        
        # Load Sierra data
        print("\n1. Loading Sierra Chart data...")
        sierra_loader = SierraChartDataLoader(sierra_csv_path)
        sierra_df = sierra_loader.load()
        
        sierra_sessions = sierra_df['VP_SessionDate'].unique().sort().to_list()
        print(f"   ✓ Found {len(sierra_sessions)} sessions in Sierra: {sierra_sessions}")
        
        # Prepare Python data
        print("\n2. Preparing Python tick data...")
        python_df = python_df.with_row_index(name='original_index')
        python_df = python_df.with_columns([
            pl.col('Datetime').map_elements(
                lambda dt: VPSession.get_vp_session_id(dt),
                return_dtype=pl.Date
            ).alias('VP_SessionDate')
        ])
        
        self.processed_python_df = python_df
        print(f"   ✓ Processed {len(python_df):,} ticks")
        
        # Validate each session
        print("\n3. Validating sessions bar-by-bar...")
        for session_date in sierra_sessions:
            try:
                print(f"\n   Session {session_date}:")
                metrics = self._validate_session(
                    python_df, sierra_df, session_date
                )
                self.session_metrics.append(metrics)
                
                status = "✓ PASS" if metrics.all_passed else "✗ FAIL"
                print(f"      {status} - {metrics.total_bars} bars")
                print(f"      POC: {metrics.poc_accuracy*100:.1f}% ({'✓' if metrics.poc_passed else '✗'})")
                print(f"      VA_Areas: {metrics.va_accuracy*100:.1f}% ({'✓' if metrics.va_passed else '✗'})")
                print(f"      Session Bounds: {metrics.session_bounds_accuracy*100:.1f}% ({'✓' if metrics.session_bounds_passed else '✗'})")
                print(f"      OHLC: {metrics.ohlc_accuracy*100:.1f}% ({'✓' if metrics.ohlc_passed else '✗'})")
                
                if not metrics.all_passed:
                    self._create_issue_from_metrics(metrics)
                    
            except Exception as e:
                print(f"      ✗ ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                self.add_issue(
                    severity=Severity.ERROR,
                    rule_name=f"Session_{session_date}_Error",
                    description=str(e)
                )
        
        passed_sessions = sum(1 for m in self.session_metrics if m.all_passed)
        execution_time = (time.perf_counter() - start_time) * 1000
        
        print(f"\n4. Summary:")
        print(f"   Sessions: {len(self.session_metrics)}")
        print(f"   Passed: {passed_sessions}")
        print(f"   Failed: {len(self.session_metrics) - passed_sessions}")
        
        return ValidationResult(
            validator_name=self.name,
            passed=(passed_sessions == len(self.session_metrics)),
            total_checks=len(self.session_metrics),
            failed_checks=len(self.session_metrics) - passed_sessions,
            issues=self.issues.copy(),
            execution_time_ms=execution_time,
            metadata={
                'sessions_total': len(self.session_metrics),
                'sessions_passed': passed_sessions,
                'total_bars_validated': sum(m.total_bars for m in self.session_metrics)
            }
        )
    
    def _validate_session(
        self,
        python_df: pl.DataFrame,
        sierra_df: pl.DataFrame,
        session_date: date
    ) -> SessionValidationMetrics:
        """Validate all bars in a session"""
        
        # Get Sierra bars for this session
        sierra_session = sierra_df.filter(pl.col('VP_SessionDate') == session_date).sort('Datetime')
        
        # Get Python ticks for this session
        python_session = python_df.filter(pl.col('VP_SessionDate') == session_date).sort('Datetime')
        
        if len(sierra_session) == 0 or len(python_session) == 0:
            raise ValueError(f"No data for session {session_date}")
        
        # Get session VP bounds
        vp_start, vp_end = VPSession.get_vp_session_bounds(session_date)
        
        # Validate each bar
        bar_results: List[BarValidationResult] = []
        
        for bar_row in sierra_session.iter_rows(named=True):
            bar_dt = bar_row['Datetime']
            
            # Define bar time window [bar_dt, bar_dt + 59.999... seconds]
            bar_start = bar_dt
            bar_end = bar_dt + timedelta(seconds=59, microseconds=999999)
            
            # Find ticks in this bar
            bar_ticks = python_session.filter(
                (pl.col('Datetime') >= bar_start) &
                (pl.col('Datetime') <= bar_end)
            )
            
            if len(bar_ticks) == 0:
                continue  # Skip bars with no ticks
            
            # Get last tick in this bar
            last_tick = bar_ticks.row(-1, named=True)
            
            # Calculate session metrics from VP start to this bar end
            session_ticks_up_to_bar = python_session.filter(
                (pl.col('Datetime') >= vp_start) &
                (pl.col('Datetime') <= bar_end)
            )
            
            calculated_high = float(session_ticks_up_to_bar['Price'].max())
            calculated_low = float(session_ticks_up_to_bar['Price'].min())
            
            # Validate this bar
            result = self._validate_bar(
                last_tick, bar_row, calculated_high, calculated_low
            )
            bar_results.append(result)
        
        # Calculate session metrics
        total_bars = len(bar_results)
        
        poc_correct = sum(1 for r in bar_results if r.poc_match)
        va_correct = sum(1 for r in bar_results if r.va_label_match)
        bounds_correct = sum(1 for r in bar_results if r.session_high_match and r.session_low_match)
        ohlc_correct = sum(1 for r in bar_results if r.ohlc_match)
        
        poc_accuracy = poc_correct / total_bars
        va_accuracy = va_correct / total_bars
        bounds_accuracy = bounds_correct / total_bars
        ohlc_accuracy = ohlc_correct / total_bars
        
        failed_bars = [r for r in bar_results if not all([
            r.poc_match, r.va_label_match, 
            r.session_high_match, r.session_low_match, 
            r.ohlc_match
        ])]
        
        return SessionValidationMetrics(
            session_date=session_date,
            total_bars=total_bars,
            poc_correct_bars=poc_correct,
            poc_accuracy=poc_accuracy,
            poc_passed=(1 - poc_accuracy) <= self.poc_tolerance_pct,
            va_correct_bars=va_correct,
            va_accuracy=va_accuracy,
            va_passed=(1 - va_accuracy) <= self.va_tolerance_pct,
            session_bounds_correct_bars=bounds_correct,
            session_bounds_accuracy=bounds_accuracy,
            session_bounds_passed=(1 - bounds_accuracy) <= self.session_bounds_tolerance_pct,
            ohlc_correct_bars=ohlc_correct,
            ohlc_accuracy=ohlc_accuracy,
            ohlc_passed=(1 - ohlc_accuracy) <= self.ohlc_tolerance_pct,
            failed_bar_results=failed_bars
        )
    
    def _validate_bar(
        self,
        last_tick: Dict,
        sierra_bar: Dict,
        calculated_high: float,
        calculated_low: float
    ) -> BarValidationResult:
        """Validate single bar's last tick against Sierra"""
        
        # POC validation
        python_poc = float(last_tick['POC'])
        sierra_poc = float(sierra_bar['POC'])
        poc_match = (python_poc == sierra_poc)
        
        # VA_Areas validation
        tick_price = float(last_tick['Price'])
        sierra_vah = float(sierra_bar['VAH'])
        sierra_val = float(sierra_bar['VAL'])
        python_label = str(last_tick['VA_Areas'])
        
        # Determine expected label
        if tick_price == sierra_poc:
            expected_label = "PO"
        elif sierra_val <= tick_price <= sierra_vah:
            expected_label = "VA"
        else:
            expected_label = "na"
        
        va_label_match = (python_label == expected_label)
        
        # Session bounds validation
        python_high = float(last_tick['Session_High'])
        python_low = float(last_tick['Session_Low'])
        session_high_match = (python_high == calculated_high)
        session_low_match = (python_low == calculated_low)
        
        # OHLC validation
        python_ohlc = {
            'open': float(last_tick['current_bar_open']),
            'high': float(last_tick['current_bar_high']),
            'low': float(last_tick['current_bar_low']),
            'close': float(last_tick['current_bar_close']),
            'volume': float(last_tick['current_bar_volume'])
        }
        
        sierra_ohlc = {
            'open': float(sierra_bar['Open']),
            'high': float(sierra_bar['High']),
            'low': float(sierra_bar['Low']),
            'close': float(sierra_bar['Last']),
            'volume': float(sierra_bar['Volume'])
        }
        
        ohlc_match = all([
            python_ohlc['open'] == sierra_ohlc['open'],
            python_ohlc['high'] == sierra_ohlc['high'],
            python_ohlc['low'] == sierra_ohlc['low'],
            python_ohlc['close'] == sierra_ohlc['close'],
            python_ohlc['volume'] == sierra_ohlc['volume']
        ])
        
        return BarValidationResult(
            bar_datetime=sierra_bar['Datetime'],
            poc_match=poc_match,
            python_poc=python_poc,
            sierra_poc=sierra_poc,
            va_label_match=va_label_match,
            python_label=python_label,
            expected_label=expected_label,
            tick_price=tick_price,
            session_high_match=session_high_match,
            session_low_match=session_low_match,
            python_high=python_high,
            python_low=python_low,
            calculated_high=calculated_high,
            calculated_low=calculated_low,
            ohlc_match=ohlc_match,
            python_ohlc=python_ohlc,
            sierra_ohlc=sierra_ohlc,
            last_tick_index=int(last_tick['original_index'])
        )
    
    def _create_issue_from_metrics(self, metrics: SessionValidationMetrics) -> None:
        """Create validation issue for failed session"""
        failed_checks = []
        
        if not metrics.poc_passed:
            failed_checks.append(
                f"POC: {metrics.poc_accuracy*100:.1f}% accuracy "
                f"({metrics.poc_correct_bars}/{metrics.total_bars} bars)"
            )
        
        if not metrics.va_passed:
            failed_checks.append(
                f"VA_Areas: {metrics.va_accuracy*100:.1f}% accuracy "
                f"({metrics.va_correct_bars}/{metrics.total_bars} bars)"
            )
        
        if not metrics.session_bounds_passed:
            failed_checks.append(
                f"Session Bounds: {metrics.session_bounds_accuracy*100:.1f}% accuracy "
                f"({metrics.session_bounds_correct_bars}/{metrics.total_bars} bars)"
            )
        
        if not metrics.ohlc_passed:
            failed_checks.append(
                f"OHLC: {metrics.ohlc_accuracy*100:.1f}% accuracy "
                f"({metrics.ohlc_correct_bars}/{metrics.total_bars} bars)"
            )
        
        self.add_issue(
            severity=Severity.ERROR,
            rule_name=f"Session_{metrics.session_date}",
            description=f"{len(failed_checks)} validation(s) failed",
            sample_data={
                'failed_checks': failed_checks,
                'failed_bars_count': len(metrics.failed_bar_results)
            }
        )
    
    def export_failed_bars(self, output_path: str) -> None:
        """Export detailed report of all failed bars"""
        if not self.session_metrics:
            print("No validation results to export")
            return
        
        data = []
        for metrics in self.session_metrics:
            for result in metrics.failed_bar_results:
                data.append({
                    'session_date': str(metrics.session_date),
                    'bar_datetime': str(result.bar_datetime),
                    'last_tick_index': result.last_tick_index,
                    
                    'poc_match': result.poc_match,
                    'python_poc': result.python_poc,
                    'sierra_poc': result.sierra_poc,
                    
                    'va_label_match': result.va_label_match,
                    'python_label': result.python_label,
                    'expected_label': result.expected_label,
                    'tick_price': result.tick_price,
                    
                    'session_high_match': result.session_high_match,
                    'python_high': result.python_high,
                    'calculated_high': result.calculated_high,
                    
                    'session_low_match': result.session_low_match,
                    'python_low': result.python_low,
                    'calculated_low': result.calculated_low,
                    
                    'ohlc_match': result.ohlc_match,
                    'python_open': result.python_ohlc['open'],
                    'sierra_open': result.sierra_ohlc['open'],
                    'python_high_bar': result.python_ohlc['high'],
                    'sierra_high_bar': result.sierra_ohlc['high'],
                    'python_low_bar': result.python_ohlc['low'],
                    'sierra_low_bar': result.sierra_ohlc['low'],
                    'python_close': result.python_ohlc['close'],
                    'sierra_close': result.sierra_ohlc['close'],
                    'python_volume': result.python_ohlc['volume'],
                    'sierra_volume': result.sierra_ohlc['volume']
                })
        
        if data:
            pl.DataFrame(data).write_csv(output_path)
            print(f"✅ Exported {len(data)} failed bars to: {output_path}")
        else:
            print("✅ No failed bars - all validations passed!")
    
    def export_summary(self, output_path: str) -> None:
        """Export session summary report"""
        data = []
        for m in self.session_metrics:
            data.append({
                'session_date': str(m.session_date),
                'total_bars': m.total_bars,
                'overall_pass': m.all_passed,
                
                'poc_accuracy_pct': m.poc_accuracy * 100,
                'poc_passed': m.poc_passed,
                
                'va_accuracy_pct': m.va_accuracy * 100,
                'va_passed': m.va_passed,
                
                'session_bounds_accuracy_pct': m.session_bounds_accuracy * 100,
                'session_bounds_passed': m.session_bounds_passed,
                
                'ohlc_accuracy_pct': m.ohlc_accuracy * 100,
                'ohlc_passed': m.ohlc_passed,
                
                'failed_bars_count': len(m.failed_bar_results)
            })
        
        pl.DataFrame(data).write_csv(output_path)
        print(f"✅ Summary report: {output_path}")


print("✅ SierraChartValidator - Bar-by-Bar Final Version")
print("   - POC validation (last tick per bar)")
print("   - VA_Areas label validation")
print("   - Session_High/Low progressive validation")
print("   - OHLC current_bar_* validation")
print("   - Configurable tolerances (% bars)")

# Load Python data
df_ticks = pl.read_csv(r'C:\Users\Tommy\Documents\PycharmProjects\Orderflow\Sources\ES\ESZ25-CME_ENR_BARS_20251212_225959.csv', separator=';')
df_ticks = df_ticks.with_columns([
    pl.col("Datetime").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f").alias("Datetime")
])

# Cast types
df_ticks = df_ticks.with_columns([
    pl.col('Price').cast(pl.Float64),
    pl.col('POC').cast(pl.Float64),
    pl.col('Session_High').cast(pl.Float64),
    pl.col('Session_Low').cast(pl.Float64),
    pl.col('VA_Areas').cast(pl.Utf8),
    pl.col('current_bar_open').cast(pl.Float64),
    pl.col('current_bar_high').cast(pl.Float64),
    pl.col('current_bar_low').cast(pl.Float64),
    pl.col('current_bar_close').cast(pl.Float64),
    pl.col('current_bar_volume').cast(pl.Int64)
])

# Validate
validator = SierraChartValidator(
    tick_size=0.25,
    poc_tolerance_pct=0.0,    # 100% accuracy
    va_tolerance_pct=0.05,    # 95% accuracy
    session_bounds_tolerance_pct=0.05,
    ohlc_tolerance_pct=0.05
)

result = validator.validate(
    python_df=df_ticks,
    sierra_csv_path=r'C:\Users\Tommy\Documents\PycharmProjects\Orderflow\Sources\Sierra\ES\ESZ25_.txt'
)

# Export
validator.export_summary('validation_summary.csv')
validator.export_failed_bars('failed_bars_detail.csv')
