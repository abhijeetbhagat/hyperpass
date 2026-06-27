use crate::error::HyperPassError;

pub trait RateLimiter: Send + Sync {
    fn process(&self, req_count: u32) -> Result<(), HyperPassError>;
}
