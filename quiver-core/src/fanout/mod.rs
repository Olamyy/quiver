pub mod failure;
pub mod merge;
pub mod metrics;

pub use failure::{detect_partial_failure, PartialFailureError};
pub use merge::{FanoutMerger, MergeError};
pub use metrics::{Backend as MetricsBackend, FanoutLatencies, Phase, Timer};
