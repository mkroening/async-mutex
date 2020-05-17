//! An async mutex.
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

use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};

use event_listener::Event;

/// An async mutex.
pub struct Mutex<T> {
    /// Set to `true` when the mutex is locked.
    locked: AtomicBool,

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
            locked: AtomicBool::new(false),
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
    /// #
    /// use async_mutex::Mutex;
    ///
    /// let mutex = Mutex::new(10);
    /// let guard = mutex.lock().await;
    /// assert_eq!(*guard, 10);
    /// #
    /// # })
    /// ```
    pub async fn lock(&self) -> MutexGuard<'_, T> {
        loop {
            // Try locking the mutex.
            if let Some(guard) = self.try_lock() {
                return guard;
            }

            // Start watching for notifications and try locking again.
            let listener = self.lock_ops.listen();
            if let Some(guard) = self.try_lock() {
                return guard;
            }
            listener.await;
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
        if !self.locked.compare_and_swap(false, true, Ordering::Acquire) {
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

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.0.locked.store(false, Ordering::Release);
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
