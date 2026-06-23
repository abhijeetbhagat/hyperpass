use crate::error::HyperPassError;
use std::time::Instant;

pub struct RateLimiter {
    capacity: f64,
    leak_rate: f64,
    tokens_count: f64,
    last_recorded_epoch: Instant,
}

impl RateLimiter {
    pub fn new(capacity: u32, leak_rate: u32) -> Self {
        Self {
            capacity: capacity as f64,
            leak_rate: leak_rate as f64,
            tokens_count: capacity as f64, // we start with all tokens available
            last_recorded_epoch: Instant::now(),
        }
    }

    pub fn process(&mut self, req_count: u32) -> Result<(), HyperPassError> {
        // check if we have enough tokens to allow the requests to be processed
        let current_epoch = std::time::Instant::now();
        let time_diff = current_epoch - self.last_recorded_epoch;
        self.last_recorded_epoch = current_epoch;

        self.tokens_count = self
            .capacity
            .min(self.tokens_count + (time_diff.as_secs_f64() * self.leak_rate));

        if req_count as f64 <= self.tokens_count {
            self.tokens_count -= req_count as f64;
            Ok(())
        } else {
            Err(HyperPassError::TooManyRequestsError)
        }
    }

    fn reset(&mut self) {
        self.tokens_count = self.capacity;
    }

    #[inline]
    fn get_tokens(&self) -> u32 {
        self.tokens_count as u32
    }
}

#[test]
fn test_rate_limiting() {
    let mut limiter = RateLimiter::new(10, 2);
    assert_eq!(limiter.process(1), Ok(()));
    assert_eq!(limiter.process(9), Ok(()));
    assert_eq!(
        limiter.process(9),
        Err(HyperPassError::TooManyRequestsError)
    );
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(limiter.process(0), Ok(()));
    assert_eq!(limiter.tokens_count as u32, 4);
}
