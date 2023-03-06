use core::sync::atomic::{AtomicBool, Ordering};
use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut};

use super::Mutex;

pub struct TASLock<T: ?Sized> {
  locked: AtomicBool,
  data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for TASLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for TASLock<T> {}

impl<T> TASLock<T> {
  fn new(t: T) -> Self {
    Self {
      locked: AtomicBool::new(false),
      data: UnsafeCell::new(t),
    }
  }
}

impl<T: ?Sized> Mutex<T> for TASLock<T> {
  type LockGuard<'a, V: ?Sized + 'a> where Self: 'a = TASLockGuard<'a, V>;

  fn lock(&self) -> Self::LockGuard<'_, T> {
    while self.locked.fetch_or(true, Ordering::Acquire) {}
    TASLockGuard::new(self)
  }

  fn unlock(&self, guard: Self::LockGuard<'_, T>) {
    drop(guard);
  }
}

pub struct TASLockGuard<'a, T: ?Sized + 'a> {
  lock: &'a TASLock<T>,
}

impl<T: ?Sized> !Send for TASLockGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for TASLockGuard<'_, T>{}

impl<'a, T: ?Sized> TASLockGuard<'a, T> {
  fn new(lock: &'a TASLock<T>) -> Self {
    Self { lock }
  }
}

impl<T: ?Sized> Drop for TASLockGuard<'_, T> {
  fn drop(&mut self) {
    self.lock.locked.store(false, Ordering::Release)
  }
}

impl<T: ?Sized> Deref for TASLockGuard<'_, T> {
  type Target = T;

  fn deref(&self) -> &T {
    unsafe { &*self.lock.data.get() }
  }
}

impl<T: ?Sized> DerefMut for TASLockGuard<'_, T> {
  fn deref_mut(&mut self) -> &mut T {
    unsafe { &mut *self.lock.data.get() }
  }
}