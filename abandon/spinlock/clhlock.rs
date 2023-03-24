use core::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut};
use core::ptr::NonNull;

use super::Mutex;

pub struct CLHNode(AtomicBool);

impl CLHNode {
  pub fn new(locked: bool) -> Self {
    Self(AtomicBool::new(locked))
  }
}

pub struct CLHLock<T: ?Sized> {
  tail: AtomicPtr<CLHNode>,
  data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for CLHLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for CLHLock<T> {}

impl<T> CLHLock<T> {
  fn new(t: T) -> Self {
    Self {
      tail: AtomicPtr::new(Box::into_raw(Box::new(CLHNode::new(false)))),      
      data: UnsafeCell::new(t),
    }
  }
}

impl<T: ?Sized> Mutex<T> for CLHLock<T> {
  type LockGuard<'a, V: ?Sized + 'a> where Self: 'a = CLHLockGuard<'a, V>;

  fn lock(&self) -> Self::LockGuard<'_, T> {
    let node = Box::into_raw(Box::new(CLHNode::new(true)));
    let prev = self.tail.swap(node, Ordering::Relaxed);
    unsafe {
      while (*prev).0.load(Ordering::Acquire) {}
    }
    CLHLockGuard::new(self, NonNull::new(node).unwrap())
  }

  fn unlock(&self, guard: Self::LockGuard<'_, T>) {
    drop(guard);
  }
}

pub struct CLHLockGuard<'a, T: ?Sized + 'a> {
  lock: &'a CLHLock<T>,
  node: NonNull<CLHNode>,
}

impl<T: ?Sized> !Send for CLHLockGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for CLHLockGuard<'_, T>{}

impl<'a, T: ?Sized> CLHLockGuard<'a, T> {
  fn new(lock: &'a CLHLock<T>, node: NonNull<CLHNode>) -> Self {
    Self { lock, node }
  }
}

impl<T: ?Sized> Drop for CLHLockGuard<'_, T> {
  fn drop(&mut self) {
    unsafe {
      (*self.node.as_ref()).0.store(false, Ordering::Release)
    } 
    // TODO: drop node
  }
}

impl<T: ?Sized> Deref for CLHLockGuard<'_, T> {
  type Target = T;

  fn deref(&self) -> &T {
    unsafe { &*self.lock.data.get() }
  }
}

impl<T: ?Sized> DerefMut for CLHLockGuard<'_, T> {
  fn deref_mut(&mut self) -> &mut T {
    unsafe { &mut *self.lock.data.get() }
  }
}