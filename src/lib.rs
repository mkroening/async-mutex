//! An async mutex.
//!
//! The locking mechanism uses eventual fairness to ensure locking will be fair on average without
//! sacrificing performance. This is done by forcing a fair lock whenever a lock operation is
//! starved for longer than 0.5 milliseconds.
//!
//! Each instance of [`Mutex`] requires 2 words of storage in addition to inner data.
//!
//! # Examples
//!
//! ```
//! # smol::run(async {
//! use async_mutex::Mutex;
//! use smol::Task;
//! use std::sync::Arc;
//!
//! let m = Arc::new(Mutex::new(0));
//! let mut tasks = vec![];
//!
//! for _ in 0..10 {
//!     let m = m.clone();
//!     tasks.push(Task::spawn(async move {
//!         *m.lock().await += 1;
//!     }));
//! }
//!
//! for t in tasks {
//!     t.await;
//! }
//! assert_eq!(*m.lock().await, 10);
//! # })
//! ```

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]

use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use event_listener::Event;

/// An async mutex.
pub struct Mutex<T> {
    /// Current state of the mutex.
    ///
    /// The least significat bit is set to 1 if the mutex is locked.
    /// The other bits hold the number of starved lock operations.
    state: AtomicUsize,

    /// Lock operations waiting for the mutex to be released.
    lock_ops: Event,

    /// The value inside the mutex.
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for Mutex<T> {}
unsafe impl<T: Send> Sync for Mutex<T> {}

impl<T> Mutex<T> {
    /// Creates a new async mutex.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(0);
    /// ```
    pub fn new(data: T) -> Mutex<T> {
        Mutex {
            state: AtomicUsize::new(0),
            lock_ops: Event::new(),
            data: UnsafeCell::new(data),
        }
    }

    /// Acquires the mutex.
    ///
    /// Returns a guard that releases the mutex when dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// # smol::block_on(async {
    /// use async_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(10);
    /// let guard = mutex.lock().await;
    /// assert_eq!(*guard, 10);
    /// # })
    /// ```
    #[inline]
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        if let Some(guard) = self.try_lock() {
            return guard;
        }
        self.lock_slow().await
    }

    /// Slow path for acquiring the mutex.
    pub async fn lock_slow(&self) -> MutexGuard<'_, T> {
        // Get the current time.
        let start = Instant::now();

        loop {
            // Start listening for events.
            let listener = self.lock_ops.listen();

            // Try locking if nobody is being starved.
            match self.state.compare_and_swap(0, 1, Ordering::Acquire) {
                // Lock acquired!
                0 => return MutexGuard(self),

                // Unlocked and somebody is starved - notify the first waiter in line.
                s if s % 2 == 0 => self.lock_ops.notify_one(),

                // The mutex is currently locked.
                _ => {}
            }

            // Wait for a notification.
            listener.await;

            // Try locking if nobody is being starved.
            match self.state.compare_and_swap(0, 1, Ordering::Acquire) {
                // Lock acquired!
                0 => return MutexGuard(self),

                // Unlocked and somebody is starved - notify the first waiter in line.
                s if s % 2 == 0 => self.lock_ops.notify_one(),

                // The mutex is currently locked.
                _ => {}
            }

            // If waiting for too long, fall back to a fairer locking strategy that will prevent
            // newer lock operations from starving us forever.
            if start.elapsed() > Duration::from_micros(500) {
                break;
            }
        }

        // Increment the number of starved lock operations.
        if self.state.fetch_add(2, Ordering::Release) > usize::MAX / 2 {
            // In case of potential overflow, abort.
            process::abort();
        }

        // Decrement the counter when exiting this function.
        let _call = CallOnDrop(|| {
            self.state.fetch_sub(2, Ordering::Release);
        });

        loop {
            // Start listening for events.
            let listener = self.lock_ops.listen();

            // Try locking if nobody else is being starved.
            match self.state.compare_and_swap(2, 2 | 1, Ordering::Acquire) {
                // Lock acquired!
                0 => return MutexGuard(self),

                // Unlocked and somebody is starved - notify the first waiter in line.
                s if s % 2 == 0 => self.lock_ops.notify_one(),

                // The mutex is currently locked.
                _ => {}
            }

            // Wait for a notification.
            listener.await;

            // Try acquiring the lock without waiting for others.
            if self.state.fetch_or(1, Ordering::Acquire) % 2 == 0 {
                return MutexGuard(self);
            }
        }
    }

    /// Attempts to acquire the mutex.
    ///
    /// If the mutex could not be acquired at this time, then [`None`] is returned. Otherwise, a
    /// guard is returned that releases the mutex when dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(10);
    /// if let Some(guard) = mutex.try_lock() {
    ///     assert_eq!(*guard, 10);
    /// }
    /// # ;
    /// ```
    #[inline]
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        if self.state.compare_and_swap(0, 1, Ordering::Acquire) == 0 {
            Some(MutexGuard(self))
        } else {
            None
        }
    }

    /// Consumes the mutex, returning the underlying data.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(10);
    /// assert_eq!(mutex.into_inner(), 10);
    /// ```
    pub fn into_inner(self) -> T {
        self.data.into_inner()
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the mutex mutably, no actual locking takes place -- the mutable
    /// borrow statically guarantees the mutex is not already acquired.
    ///
    /// # Examples
    ///
    /// ```
    /// # smol::block_on(async {
    /// use async_mutex::Mutex;
    ///
    /// let mut mutex = Mutex::new(0);
    /// *mutex.get_mut() = 10;
    /// assert_eq!(*mutex.lock().await, 10);
    /// # })
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.data.get() }
    }
}

impl<T: fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Locked;
        impl fmt::Debug for Locked {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("<locked>")
            }
        }

        match self.try_lock() {
            None => f.debug_struct("Mutex").field("data", &Locked).finish(),
            Some(guard) => f.debug_struct("Mutex").field("data", &&*guard).finish(),
        }
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(val: T) -> Mutex<T> {
        Mutex::new(val)
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Mutex<T> {
        Mutex::new(Default::default())
    }
}

/// A guard that releases the mutex when dropped.
pub struct MutexGuard<'a, T>(&'a Mutex<T>);

unsafe impl<T: Send> Send for MutexGuard<'_, T> {}
unsafe impl<T: Sync> Sync for MutexGuard<'_, T> {}

impl<'a, T> MutexGuard<'a, T> {
    /// Returns a reference to the mutex a guard came from.
    ///
    /// # Examples
    ///
    /// ```
    /// # smol::block_on(async {
    /// use async_mutex::{Mutex, MutexGuard};
    ///
    /// let mutex = Mutex::new(10i32);
    /// let guard = mutex.lock().await;
    /// dbg!(MutexGuard::source(&guard));
    /// # })
    /// ```
    pub fn source(guard: &MutexGuard<'a, T>) -> &'a Mutex<T> {
        guard.0
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        // Remove the last bit and notify a waiting lock operation.
        self.0.state.fetch_sub(1, Ordering::Release);
        self.0.lock_ops.notify_one();
    }
}

impl<T: fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.0.data.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.0.data.get() }
    }
}

struct CallOnDrop<F: Fn()>(F);

impl<F: Fn()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}
