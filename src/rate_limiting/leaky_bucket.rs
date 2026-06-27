use crate::error::HyperPassError;
use crate::rate_limiting::RateLimiter;
use std::{
    sync::atomic::{AtomicU128, Ordering},
    time::Instant,
};

pub struct LeakyBucketRateLimiter {
    capacity: f64,
    leak_rate: f64,
    time_and_level: AtomicU128,
    rate_limiter_start_epoch: Instant,
}

impl LeakyBucketRateLimiter {
    pub fn new(capacity: u32, leak_rate: u32) -> Self {
        Self {
            capacity: capacity as f64,
            leak_rate: leak_rate as f64,
            time_and_level: AtomicU128::new(pack(0, 0f64)), // we start with an empty bucket
            rate_limiter_start_epoch: Instant::now(),
        }
    }
}

impl RateLimiter for LeakyBucketRateLimiter {
    fn process(&self, req_count: u32) -> Result<(), HyperPassError> {
        let current_epoch = std::time::Instant::now();
        let time_diff = (current_epoch - self.rate_limiter_start_epoch).as_nanos() as u64;

        let mut current_time_and_level = self
            .time_and_level
            .load(std::sync::atomic::Ordering::Acquire);

        loop {
            let (last_updated_epoch, level) = unpack(current_time_and_level);
            let delta_secs = (time_diff - last_updated_epoch) as f64 / 1_000_000_000f64;
            let leaked_level = (0f64).max(level - (delta_secs * self.leak_rate));
            let added_level = leaked_level + req_count as f64;

            if self.capacity >= added_level {
                let new_time_and_level = pack(time_diff, added_level);

                match self.time_and_level.compare_exchange_weak(
                    current_time_and_level,
                    new_time_and_level,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(old_value) => current_time_and_level = old_value,
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
    let limiter = LeakyBucketRateLimiter::new(10, 2);
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
    let limiter = LeakyBucketRateLimiter::new(5, 2);
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
