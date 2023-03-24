// https://zhuanlan.zhihu.com/p/80727111

use core::sync::atomic::{AtomicU8, Ordering};
use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut};

use super::Mutex;

pub struct TicketLock<T: ?Sized> {
  head: AtomicU8,
  tail: AtomicU8,
  data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for TicketLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for TicketLock<T> {}

impl<T> TicketLock<T> {
  fn new(t: T) -> Self {
    Self {
      head: AtomicU8::new(0),
      tail: AtomicU8::new(0),
      data: UnsafeCell::new(t),
    }
  }
}

impl<T: ?Sized> Mutex<T> for TicketLock<T> {
  type LockGuard<'a, V: ?Sized + 'a> where Self: 'a = TicketLockGuard<'a, V>;

  fn lock(&self) -> Self::LockGuard<'_, T> {
    let old_tail = self.tail.fetch_add(1, Ordering::Release);
    while self.head.load(Ordering::Acquire) != old_tail {}
    TicketLockGuard::new(self)
  }

  fn unlock(&self, guard: Self::LockGuard<'_, T>) {
    drop(guard);
  }
}

pub struct TicketLockGuard<'a, T: ?Sized + 'a> {
  lock: &'a TicketLock<T>,
}

impl<T: ?Sized> !Send for TicketLockGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for TicketLockGuard<'_, T>{}

impl<'a, T: ?Sized> TicketLockGuard<'a, T> {
  fn new(lock: &'a TicketLock<T>) -> Self {
    Self { lock }
  }
}

impl<T: ?Sized> Drop for TicketLockGuard<'_, T> {
  fn drop(&mut self) {
    self.lock.tail.fetch_add(1, Ordering::Release);
  }
}

impl<T: ?Sized> Deref for TicketLockGuard<'_, T> {
  type Target = T;

  fn deref(&self) -> &T {
    unsafe { &*self.lock.data.get() }
  }
}

impl<T: ?Sized> DerefMut for TicketLockGuard<'_, T> {
  fn deref_mut(&mut self) -> &mut T {
    unsafe { &mut *self.lock.data.get() }
  }
}