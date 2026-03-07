use std::time::{Duration, Instant};

/// Timeout budget manager for distributed timeout handling across adapters and operations.
///
/// This utility helps ensure that total request time doesn't exceed intended limits
/// by distributing timeout budgets across multiple operations intelligently.
#[derive(Clone)]
pub struct TimeoutBudget {
    start_time: Instant,
    total_timeout: Duration,
}

impl TimeoutBudget {
    /// Create a new timeout budget with the specified total duration.
    pub fn new(total_timeout: Duration) -> Self {
        Self {
            start_time: Instant::now(),
            total_timeout,
        }
    }

    /// Get remaining time in the budget.
    /// Returns None if timeout has already been exceeded.
    pub fn remaining(&self) -> Option<Duration> {
        let elapsed = self.start_time.elapsed();
        if elapsed >= self.total_timeout {
            None
        } else {
            Some(self.total_timeout - elapsed)
        }
    }

    /// Check if timeout has been exceeded.
    pub fn is_exceeded(&self) -> bool {
        self.start_time.elapsed() >= self.total_timeout
    }

    /// Get elapsed time since budget creation.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get total timeout duration.
    pub fn total(&self) -> Duration {
        self.total_timeout
    }

    /// Distribute remaining timeout across N operations.
    /// Returns timeout per operation, ensuring no operation gets more than remaining time.
    pub fn distribute_remaining(&self, operation_count: usize) -> Option<Duration> {
        if operation_count == 0 {
            return self.remaining();
        }

        let remaining = self.remaining()?;
        Some(remaining / operation_count as u32)
    }

    /// Get timeout for a single adapter operation with safety margins.
    ///
    /// This method reserves a small buffer for request overhead while giving
    /// the adapter most of the remaining time budget.
    pub fn adapter_timeout(&self, safety_margin_ms: u64) -> Option<Duration> {
        let remaining = self.remaining()?;
        let safety_margin = Duration::from_millis(safety_margin_ms);

        if remaining <= safety_margin {
            None
        } else {
            Some(remaining - safety_margin)
        }
    }

    /// Create sub-budget for a specific operation.
    /// This is useful when one operation needs a specific amount of time
    /// and remaining operations need to share the rest.
    pub fn allocate(&self, allocation: Duration) -> Option<TimeoutBudget> {
        let remaining = self.remaining()?;
        if allocation > remaining {
            None
        } else {
            Some(TimeoutBudget::new(allocation))
        }
    }

    /// Get connection acquire timeout based on operation timeout.
    /// Returns a reasonable fraction of operation timeout for connection acquisition.
    pub fn connection_timeout(&self) -> Option<Duration> {
        let remaining = self.remaining()?;
        // Use 25% of remaining time or max 5 seconds for connection acquisition
        let fraction_timeout = remaining / 4;
        let max_connection_timeout = Duration::from_secs(5);
        Some(fraction_timeout.min(max_connection_timeout))
    }
}

/// Timeout helper for periodic timeout checks during long-running operations.
#[derive(Clone)]
pub struct TimeoutChecker {
    budget: TimeoutBudget,
    check_interval: Duration,
    last_check: Instant,
}

impl TimeoutChecker {
    /// Create a new timeout checker with specified check interval.
    pub fn new(budget: TimeoutBudget, check_interval: Duration) -> Self {
        Self {
            budget,
            check_interval,
            last_check: Instant::now(),
        }
    }

    /// Check if timeout should be evaluated (based on check interval).
    /// Returns true if enough time has passed since last check.
    pub fn should_check(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_check) >= self.check_interval {
            self.last_check = now;
            true
        } else {
            false
        }
    }

    /// Perform timeout check if interval has passed.
    /// Returns true if timeout exceeded, false if still within budget.
    pub fn check(&mut self) -> bool {
        if self.should_check() {
            self.budget.is_exceeded()
        } else {
            false
        }
    }

    /// Force timeout check regardless of interval.
    pub fn force_check(&self) -> bool {
        self.budget.is_exceeded()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_timeout_budget_creation() {
        let budget = TimeoutBudget::new(Duration::from_secs(10));
        assert_eq!(budget.total(), Duration::from_secs(10));
        assert!(!budget.is_exceeded());
    }

    #[test]
    fn test_timeout_budget_remaining() {
        let budget = TimeoutBudget::new(Duration::from_secs(10));
        let remaining = budget.remaining().unwrap();
        assert!(remaining <= Duration::from_secs(10));
        assert!(remaining >= Duration::from_secs(9)); // Should be close to 10s
    }

    #[test]
    fn test_timeout_budget_distribution() {
        let budget = TimeoutBudget::new(Duration::from_secs(10));
        let per_op = budget.distribute_remaining(5).unwrap();
        assert!(per_op <= Duration::from_secs(2));
        assert!(per_op >= Duration::from_millis(1800)); // Should be close to 2s
    }

    #[test]
    fn test_adapter_timeout_with_margin() {
        let budget = TimeoutBudget::new(Duration::from_secs(10));
        let adapter_timeout = budget.adapter_timeout(500).unwrap();
        let expected_max = Duration::from_secs(10) - Duration::from_millis(500);
        assert!(adapter_timeout <= expected_max);
        assert!(adapter_timeout >= Duration::from_secs(9));
    }

    #[test]
    fn test_connection_timeout() {
        let budget = TimeoutBudget::new(Duration::from_secs(20));
        let conn_timeout = budget.connection_timeout().unwrap();
        assert!(conn_timeout <= Duration::from_secs(5)); // Max 5s
        assert!(conn_timeout >= Duration::from_secs(4)); // Should be close to 5s for 20s budget
    }

    #[test]
    fn test_timeout_checker() {
        let budget = TimeoutBudget::new(Duration::from_millis(100));
        let mut checker = TimeoutChecker::new(budget, Duration::from_millis(10));

        assert!(!checker.check()); // Should not be exceeded immediately

        thread::sleep(Duration::from_millis(110));
        assert!(checker.check()); // Should be exceeded after sleep
    }

    #[test]
    fn test_timeout_exceeded() {
        let budget = TimeoutBudget::new(Duration::from_millis(1));
        thread::sleep(Duration::from_millis(5));
        assert!(budget.is_exceeded());
        assert!(budget.remaining().is_none());
    }
}
