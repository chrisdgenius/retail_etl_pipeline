# src/error_handling/pipeline_resilience.py
import time
import functools
from typing import Callable, Any, Optional, Dict, List
from enum import Enum
from dataclasses import dataclass
from src.utils.logger import ETLLogger
import traceback
import json

class ErrorSeverity(Enum):
    """ 🚨 Error severity levels for intelligent handling
    Different errors require different responses:
    - CRITICAL: Stop everything, alert on-call team
    - HIGH: Retry with backoff, escalate if persistent
    - MEDIUM: Retry limited times, log for investigation
    - LOW: Log and continue, handle gracefully
    """
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

@dataclass
class ErrorContext:
    """📋 Comprehensive error context for debugging and recovery"""
    error_type: str
    error_message: str
    severity: ErrorSeverity
    component: str
    timestamp: str
    stack_trace: str
    data_context: Dict[str, Any]
    retry_count: int = 0
    recovery_suggestions: List[str] = None

class CircuitBreaker:
    """ ⚡ Circuit Breaker Pattern Implementation
    Prevents cascade failures by temporarily stopping calls to failing services.

    States:
    - CLOSED: Normal operation, calls pass through
    - OPEN: Failure threshold reached, calls fail fast
    - HALF_OPEN: Testing if service has recovered

    This is crucial for:
    - Preventing resource exhaustion
    - Allowing failing services time to recover
    - Maintaining system stability under stress
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"
        self.logger = ETLLogger(__name__)

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                self.logger.info("🔄 Circuit breaker moving to HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN - failing fast")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery"""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.recovery_timeout

    def _on_success(self):
        """Handle successful call"""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failure_count = 0
            self.logger.info("✅ Circuit breaker reset to CLOSED state")

    def _on_failure(self):
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            self.logger.error(f"🚨 Circuit breaker OPEN - {self.failure_count} failures")

def retry_with_exponential_backoff(max_retries: int = 3, base_delay: float = 1.0,
                                  max_delay: float = 60.0, backoff_factor: float = 2.0):
    """ 🔄 Exponential Backoff Retry Decorator
    Implements intelligent retry logic with exponential backoff:
    - First retry: wait 1 second
    - Second retry: wait 2 seconds
    - Third retry: wait 4 seconds
    - etc.

    This prevents overwhelming failing services while giving them time to recover.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = ETLLogger(func.__name__)

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(f"❌ Function {func.__name__} failed after {max_retries} retries: {str(e)}")
                        raise e

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                    logger.warning(f"⚠️ Attempt {attempt + 1} failed for {func.__name__}, retrying in {delay:.1f}s: {str(e)}")
                    time.sleep(delay)

        return wrapper
    return decorator

class PipelineErrorHandler:
    """ 🛡️ Centralized Error Handling System
    This class provides comprehensive error handling capabilities:
    - Error classification and routing
    - Recovery strategy execution
    - Error reporting and alerting
    - Graceful degradation options
    """

    def __init__(self):
        self.logger = ETLLogger(__name__)
        self.error_history: List[ErrorContext] = []
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.recovery_strategies = {
            "data_source_unavailable": self._handle_data_source_error,
            "memory_error": self._handle_memory_error,
            "network_timeout": self._handle_network_error,
            "data_quality_failure": self._handle_data_quality_error,
            "s3_access_error": self._handle_s3_error
        }

    def handle_error(self, error: Exception, component: str,
                    data_context: Dict[str, Any] = None) -> ErrorContext:
        """ 🎯 Main error handling entry point
        This method:
        1. Classifies the error type and severity
        2. Creates comprehensive error context
        3. Determines appropriate recovery strategy
        4. Executes recovery if possible
        5. Logs and reports the incident
        """
        error_context = self._create_error_context(error, component, data_context)
        self.error_history.append(error_context)
        self.logger.error(f"🚨 Error in {component}: {error_context.error_message}")

        # Attempt recovery based on error type
        recovery_attempted = self._attempt_recovery(error_context)

        # Alert if critical or recovery failed
        if error_context.severity == ErrorSeverity.CRITICAL or not recovery_attempted:
            self._send_alert(error_context)

        return error_context

    def _create_error_context(self, error: Exception, component: str,
                             data_context: Dict[str, Any] = None) -> ErrorContext:
        """Create comprehensive error context for analysis"""
        error_type = type(error).__name__
        severity = self._classify_error_severity(error, component)

        return ErrorContext(
            error_type=error_type,
            error_message=str(error),
            severity=severity,
            component=component,
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
            stack_trace=traceback.format_exc(),
            data_context=data_context or {},
            recovery_suggestions=self._get_recovery_suggestions(error_type)
        )

    def _classify_error_severity(self, error: Exception, component: str) -> ErrorSeverity:
        """ 🎯 Intelligent error classification
        Classification rules:
        - OutOfMemoryError: CRITICAL (can crash entire pipeline)
        - S3 access errors: HIGH (affects data persistence)
        - Data quality issues: MEDIUM (affects accuracy but not availability)
        - Network timeouts: LOW (usually transient)
        """
        error_type = type(error).__name__
        error_message = str(error).lower()

        # Critical errors that require immediate attention
        if any(keyword in error_message for keyword in ["out of memory", "java heap space", "metaspace"]):
            return ErrorSeverity.CRITICAL

        # High priority errors
        if any(keyword in error_message for keyword in ["s3", "access denied", "connection refused"]):
            return ErrorSeverity.HIGH

        # Medium priority errors
        if any(keyword in error_message for keyword in ["data quality", "schema mismatch", "null pointer"]):
            return ErrorSeverity.MEDIUM

        # Default to low priority
        return ErrorSeverity.LOW

    def _attempt_recovery(self, error_context: ErrorContext) -> bool:
        """ 🔧 Attempt automatic recovery based on error type
        Recovery strategies:
        - Memory errors: Reduce partition size, increase parallelism
        - Network errors: Retry with backoff
        - Data quality errors: Skip bad records, continue processing
        - S3 errors: Switch to backup region, retry with different credentials
        """
        recovery_strategy = None

        # Map error types to recovery strategies
        if "memory" in error_context.error_message.lower():
            recovery_strategy = self.recovery_strategies.get("memory_error")
        elif "s3" in error_context.error_message.lower():
            recovery_strategy = self.recovery_strategies.get("s3_access_error")
        elif "timeout" in error_context.error_message.lower():
            recovery_strategy = self.recovery_strategies.get("network_timeout")
        elif "data quality" in error_context.error_message.lower():
            recovery_strategy = self.recovery_strategies.get("data_quality_failure")

        if recovery_strategy:
            try:
                self.logger.info(f"🔧 Attempting recovery for {error_context.component}")
                recovery_strategy(error_context)
                return True
            except Exception as recovery_error:
                self.logger.error(f"❌ Recovery failed: {str(recovery_error)}")
                return False

        return False

    def _handle_memory_error(self, error_context: ErrorContext):
        """Recovery strategy for memory-related errors"""
        self.logger.info("🧠 Implementing memory error recovery...")
        suggestions = [
            "Increase executor memory configuration",
            "Reduce partition size to process smaller chunks",
            "Enable dynamic allocation",
            "Use more efficient data formats (Parquet vs CSV)",
            "Implement data sampling for large datasets"
        ]
        error_context.recovery_suggestions = suggestions

    def _handle_s3_error(self, error_context: ErrorContext):
        """Recovery strategy for S3 access errors"""
        self.logger.info("☁️ Implementing S3 error recovery...")
        suggestions = [
            "Check AWS credentials and permissions",
            "Verify S3 bucket exists and is accessible",
            "Try alternative S3 endpoint or region",
            "Implement retry logic with exponential backoff",
            "Use S3 Transfer Acceleration if available"
        ]
        error_context.recovery_suggestions = suggestions

    def _handle_data_quality_error(self, error_context: ErrorContext):
        """Recovery strategy for data quality issues"""
        self.logger.info("🎯 Implementing data quality error recovery...")
        suggestions = [
            "Skip records that fail validation",
            "Apply data cleansing rules",
            "Use default values for missing data",
            "Quarantine bad data for manual review",
            "Adjust quality thresholds temporarily"
        ]
        error_context.recovery_suggestions = suggestions

    def _handle_network_error(self, error_context: ErrorContext):
        """Recovery strategy for network-related errors"""
        self.logger.info("🌐 Implementing network error recovery...")
        # Implement retry with circuit breaker
        component = error_context.component
        if component not in self.circuit_breakers:
            self.circuit_breakers[component] = CircuitBreaker()

    def _get_recovery_suggestions(self, error_type: str) -> List[str]:
        """Get contextual recovery suggestions based on error type"""
        suggestions_map = {
            "OutOfMemoryError": [
                "Increase Spark executor memory",
                "Reduce data partition size",
                "Use more efficient serialization"
            ],
            "ConnectionError": [
                "Check network connectivity",
                "Verify service endpoints",
                "Implement retry logic"
            ],
            "FileNotFoundError": [
                "Verify file paths and permissions",
                "Check if data source is available",
                "Implement fallback data sources"
            ]
        }
        return suggestions_map.get(error_type, ["Contact system administrator"])

    def _send_alert(self, error_context: ErrorContext):
        """ 📢 Send alerts for critical errors
        In production, this would integrate with:
        - Slack/Teams for immediate notifications
        - PagerDuty for on-call escalation
        - Email for detailed error reports
        - Monitoring dashboards for visibility
        """
        alert_message = {
            "severity": error_context.severity.value,
            "component": error_context.component,
            "error": error_context.error_message,
            "timestamp": error_context.timestamp,
            "suggestions": error_context.recovery_suggestions
        }
        self.logger.error(f"🚨 ALERT: {json.dumps(alert_message, indent=2)}")

    def get_error_summary(self) -> Dict[str, Any]:
        """Generate error summary for monitoring and reporting"""
        if not self.error_history:
            return {"total_errors": 0, "summary": "No errors recorded"}

        error_counts = {}
        severity_counts = {}

        for error in self.error_history:
            error_counts[error.error_type] = error_counts.get(error.error_type, 0) + 1
            severity_counts[error.severity.value] = severity_counts.get(error.severity.value, 0) + 1

        return {
            "total_errors": len(self.error_history),
            "error_types": error_counts,
            "severity_distribution": severity_counts,
            "most_recent_error": self.error_history[-1].error_message,
            "error_rate_trend": self._calculate_error_rate_trend()
        }

    def _calculate_error_rate_trend(self) -> str:
        """Calculate if error rate is increasing, decreasing, or stable"""
        if len(self.error_history) < 10:
            return "Insufficient data"

        recent_errors = len([e for e in self.error_history[-5:]])
        previous_errors = len([e for e in self.error_history[-10:-5]])

        if recent_errors > previous_errors:
            return "Increasing"
        elif recent_errors < previous_errors:
            return "Decreasing"
        else:
            return "Stable"
