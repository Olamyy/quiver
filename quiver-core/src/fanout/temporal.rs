/// Clock-skew aware temporal routing for multi-backend fanout.
///
/// Per RFC v0.3 § 4.2, this module implements decision logic for selecting
/// which backends to query based on temporal requirements and TTL bounds.
///
/// Key concepts:
/// - **TTL boundary race condition:** When as_of approaches backend.now() - ttl,
///   the data may or may not be available depending on clock synchronization
/// - **Clock skew margin:** Default 30s margin to account for NTP drift
/// - **Lazy fallback:** Try primary backend first, use fallback on timeout/empty result

use chrono::{DateTime, Utc};
use std::time::Duration;

/// Temporal compatibility check result for a backend.
///
/// Determines whether a backend is suitable for serving data at a given as_of timestamp
/// based on its TTL, clock skew characteristics, and recency guarantees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemporalCompatibility {
    /// Backend can definitely serve data at this as_of (safe margin).
    Safe,

    /// Backend might serve data (at TTL boundary, subject to clock skew).
    /// Requires lazy fallback strategy: try this backend first, fallback if empty.
    AtRisk,

    /// Backend cannot serve data at this as_of (past its TTL).
    Unavailable,
}

/// Clock skew-aware temporal routing decision.
///
/// Helps resolver select which backends to try first, which to use as fallback,
/// and which to skip entirely based on temporal semantics.
pub struct TemporalRouter {
    /// Clock skew margin in seconds (default 30s per RFC v0.3).
    /// Added to all TTL calculations to account for NTP drift between servers.
    pub clock_skew_margin_seconds: u32,
}

impl TemporalRouter {
    /// Create a router with default 30-second clock skew margin.
    pub fn new() -> Self {
        Self {
            clock_skew_margin_seconds: 30,
        }
    }

    /// Create a router with custom clock skew margin.
    pub fn with_clock_skew(clock_skew_margin_seconds: u32) -> Self {
        Self {
            clock_skew_margin_seconds,
        }
    }

    /// Check if a backend is compatible for serving data at the given as_of time.
    ///
    /// # Arguments
    ///
    /// * `as_of` - The point-in-time for the query (None = current time)
    /// * `now` - Current server time (used as reference for recency calculations)
    /// * `ttl_seconds` - Backend's TTL for this feature
    ///
    /// # Returns
    ///
    /// `Safe` if data is definitely available with safety margin.
    /// `AtRisk` if data might be available (at TTL boundary, needs fallback).
    /// `Unavailable` if data is past backend's TTL.
    ///
    /// # Example
    ///
    /// Backend has 1-hour TTL, query is for data 50 minutes old:
    /// - Without clock skew: Safe (10 min margin)
    /// - With 5min clock skew: Still safe
    /// - With 15min clock skew: AtRisk
    /// - With 65min clock skew: Unavailable
    pub fn check_compatibility(
        &self,
        as_of: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        ttl_seconds: Option<u32>,
    ) -> TemporalCompatibility {
        let as_of = as_of.unwrap_or(now);

        // If no TTL constraint, backend can serve (temporal or full history)
        let ttl_seconds = match ttl_seconds {
            None => return TemporalCompatibility::Safe,
            Some(t) => t,
        };

        let age_seconds = (now - as_of).num_seconds().max(0) as u32;

        // TTL boundary: when is data guaranteed to be unavailable?
        // With clock skew margin, adjust both boundaries:
        // - Safe: age <= (ttl - margin) - guaranteed safe
        // - AtRisk: (ttl - margin) < age <= ttl - might be available
        // - Unavailable: age > ttl - definitely unavailable
        let safe_ttl = ttl_seconds.saturating_sub(self.clock_skew_margin_seconds);
        let max_ttl = ttl_seconds;

        if age_seconds <= safe_ttl {
            TemporalCompatibility::Safe
        } else if age_seconds <= max_ttl {
            TemporalCompatibility::AtRisk
        } else {
            TemporalCompatibility::Unavailable
        }
    }

    /// Determine fallback strategy for a single backend.
    ///
    /// Used during fanout execution to decide:
    /// - Try this backend (primary or fallback)
    /// - Skip this backend
    /// - Use as fallback only
    ///
    /// # Returns
    ///
    /// `Some(priority)` where priority 0 = try first, 1 = first fallback, etc.
    /// `None` if backend should not be used.
    pub fn backend_priority(
        &self,
        compatibility: TemporalCompatibility,
    ) -> Option<u32> {
        match compatibility {
            TemporalCompatibility::Safe => Some(0),    // Primary
            TemporalCompatibility::AtRisk => Some(1),  // Fallback
            TemporalCompatibility::Unavailable => None, // Skip
        }
    }
}

impl Default for TemporalRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_compatibility_no_ttl() {
        let router = TemporalRouter::new();
        let now = Utc::now();
        let as_of = now - Duration::from_secs(3600);

        let result = router.check_compatibility(Some(as_of), now, None);
        assert_eq!(result, TemporalCompatibility::Safe);
    }

    #[test]
    fn test_check_compatibility_safe_margin() {
        let router = TemporalRouter::new();
        let now = Utc::now();
        let as_of = now - Duration::from_secs(600); // 10 minutes ago

        let result = router.check_compatibility(Some(as_of), now, Some(3600)); // 1-hour TTL
        assert_eq!(result, TemporalCompatibility::Safe);
    }

    #[test]
    fn test_check_compatibility_at_risk() {
        let router = TemporalRouter::new();
        let now = Utc::now();
        // 59 minutes old: beyond safe margin (59m 30s - 30s = 59m) but within TTL (1hr)
        // safe_ttl = 3600 - 30 = 3570 (59m 30s)
        // max_ttl = 3600 (60m)
        // 59m = 3540s is within safe_ttl, so this should be Safe
        // 59.5m = 3570s is exactly at boundary
        // For AtRisk, we need age > safe_ttl and age <= max_ttl
        // Let's use 59m 45s = 3585s
        let as_of = now - Duration::from_secs(3585);

        let result = router.check_compatibility(Some(as_of), now, Some(3600));
        assert_eq!(result, TemporalCompatibility::AtRisk);
    }

    #[test]
    fn test_check_compatibility_unavailable() {
        let router = TemporalRouter::new();
        let now = Utc::now();
        let as_of = now - Duration::from_secs(4000); // 66+ minutes ago

        let result = router.check_compatibility(Some(as_of), now, Some(3600)); // 1-hour TTL
        assert_eq!(result, TemporalCompatibility::Unavailable);
    }

    #[test]
    fn test_check_compatibility_current_time() {
        let router = TemporalRouter::new();
        let now = Utc::now();

        let result = router.check_compatibility(None, now, Some(3600));
        assert_eq!(result, TemporalCompatibility::Safe);
    }

    #[test]
    fn test_backend_priority_safe() {
        let router = TemporalRouter::new();
        let priority = router.backend_priority(TemporalCompatibility::Safe);
        assert_eq!(priority, Some(0));
    }

    #[test]
    fn test_backend_priority_at_risk() {
        let router = TemporalRouter::new();
        let priority = router.backend_priority(TemporalCompatibility::AtRisk);
        assert_eq!(priority, Some(1));
    }

    #[test]
    fn test_backend_priority_unavailable() {
        let router = TemporalRouter::new();
        let priority = router.backend_priority(TemporalCompatibility::Unavailable);
        assert_eq!(priority, None);
    }

    #[test]
    fn test_custom_clock_skew() {
        let router = TemporalRouter::with_clock_skew(60); // 60s margin
        let now = Utc::now();
        let as_of = now - Duration::from_secs(31 * 60); // 31 min old

        // With 60s margin, safe_ttl = 3600 - 60 = 3540s (59 min)
        // 31 min (1860s) is within safe margin
        let result = router.check_compatibility(Some(as_of), now, Some(3600));
        assert_eq!(result, TemporalCompatibility::Safe);
    }

    #[test]
    fn test_at_boundary() {
        let router = TemporalRouter::new(); // 30s margin
        let now = Utc::now();
        // exactly at safe_ttl boundary: 3600 - 30 = 3570 (59m 30s)
        let as_of = now - Duration::from_secs(3570);

        let result = router.check_compatibility(Some(as_of), now, Some(3600));
        // age_seconds = 3570
        // safe_ttl = 3570
        // 3570 <= 3570 is true, so Safe (inclusive boundary)
        assert_eq!(result, TemporalCompatibility::Safe);
    }

    #[test]
    fn test_future_as_of() {
        let router = TemporalRouter::new();
        let now = Utc::now();
        let as_of = now + Duration::from_secs(3600); // 1 hour in future

        let result = router.check_compatibility(Some(as_of), now, Some(3600));
        // Future query treated as age_seconds = 0, which is always safe
        assert_eq!(result, TemporalCompatibility::Safe);
    }
}
