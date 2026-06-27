use crate::error::HyperPassError;
use crate::rate_limiting::RateLimiter;
use std::{
    sync::atomic::{AtomicU128, Ordering},
    time::Instant,
};

/// A lock-free token bucket rate limiter
pub struct TokenBucketRateLimiter {
    capacity: f64,
    leak_rate: f64,
    time_and_tokens: AtomicU128,
    rate_limiter_start_epoch: Instant,
}

impl TokenBucketRateLimiter {
    pub fn new(capacity: u32, leak_rate: u32) -> Self {
        Self {
            capacity: capacity as f64,
            leak_rate: leak_rate as f64,
            time_and_tokens: AtomicU128::new(pack(0, capacity as f64)), // we start with all tokens available
            rate_limiter_start_epoch: Instant::now(),
        }
    }
}

impl RateLimiter for TokenBucketRateLimiter {
    fn process(&self, req_count: u32) -> Result<(), HyperPassError> {
        let current_epoch = std::time::Instant::now();
        let time_diff = (current_epoch - self.rate_limiter_start_epoch).as_nanos() as u64;

        let mut current_time_and_tokens = self
            .time_and_tokens
            .load(std::sync::atomic::Ordering::Acquire);

        loop {
            let (last_updated_epoch, tokens_count) = unpack(current_time_and_tokens);
            let delta_secs = (time_diff - last_updated_epoch) as f64 / 1_000_000_000f64;
            let needed_tokens = (self.capacity).min(tokens_count + (delta_secs * self.leak_rate));

            if req_count as f64 <= needed_tokens {
                let token_delta = needed_tokens - req_count as f64;
                let new_time_and_tokens = pack(time_diff, token_delta);

                match self.time_and_tokens.compare_exchange_weak(
                    current_time_and_tokens,
                    new_time_and_tokens,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(old_value) => current_time_and_tokens = old_value,
                }
            } else {
                return Err(HyperPassError::TooManyRequestsError);
            }
        }

        Ok(())
    }

    // fn reset(&mut self) {
    //     self.tokens_count = self.capacity;
    // }

    // #[inline]
    // fn get_tokens(&self) -> u32 {
    //     self.tokens_count as u32
    // }
}

#[inline]
fn pack(time: u64, count: f64) -> u128 {
    ((time as u128) << 64) | (count.to_bits() as u128)
}

#[inline]
fn unpack(time_and_count: u128) -> (u64, f64) {
    (
        (time_and_count >> 64) as u64,
        (f64::from_bits(time_and_count as u64)),
    )
}

#[test]
fn test_rate_limiting() {
    let limiter = TokenBucketRateLimiter::new(10, 2);
    assert_eq!(limiter.process(1), Ok(()));
    assert_eq!(limiter.process(9), Ok(()));
    assert_eq!(
        limiter.process(9),
        Err(HyperPassError::TooManyRequestsError)
    );
    std::thread::sleep(std::time::Duration::from_secs(2));
    assert_eq!(limiter.process(0), Ok(()));
    // assert_eq!(limiter.tokens_count as u32, 4);
}

#[test]
fn test_rate_limiting_2() {
    let limiter = TokenBucketRateLimiter::new(5, 2);
    assert_eq!(limiter.process(1), Ok(()));
    assert_eq!(limiter.process(1), Ok(()));
    assert_eq!(limiter.process(1), Ok(()));
    assert_eq!(limiter.process(1), Ok(()));
    assert_eq!(limiter.process(1), Ok(()));
    assert_eq!(
        limiter.process(1),
        Err(HyperPassError::TooManyRequestsError)
    );
    assert_eq!(
        limiter.process(1),
        Err(HyperPassError::TooManyRequestsError)
    );
    assert_eq!(
        limiter.process(1),
        Err(HyperPassError::TooManyRequestsError)
    );
}
