// https://zhuanlan.zhihu.com/p/89058726

use core::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut};
use core::ptr::NonNull;

use super::Mutex;

pub struct MCSNode {
  locked: bool,
  next: *mut MCSNode,
}

impl MCSNode {
  pub fn new(locked: bool) -> MCSNode {
    Self {
      locked,
      next: core::ptr::null_mut(),
    }
  }
}

pub struct MCSLock<T: ?Sized> {
  tail: AtomicPtr<MCSNode>,
  data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for MCSLock<T> {}
unsafe impl<T: ?Sized + Send> Sync for MCSLock<T> {}

impl<T> MCSLock<T> {
  fn new(t: T) -> Self {
    Self {
      tail: AtomicPtr::new(core::ptr::null_mut()), 
      data: UnsafeCell::new(t),
    }
  }
}

impl<T: ?Sized> Mutex<T> for MCSLock<T> {
  type LockGuard<'a, V: ?Sized + 'a> where Self: 'a = MCSLockGuard<'a, V>;

  fn lock(&self) -> Self::LockGuard<'_, T> {
    let node = Box::into_raw(Box::new(MCSNode::new(false)));
    let pred = self.tail.swap(node, Ordering::Acquire);
    if !pred.is_null() {
      unsafe { 
        (&mut *node).locked = true;
        (&mut *pred).next = node;
        while (&*node).locked {}
      }
    }
    MCSLockGuard::new(self, NonNull::new(node).unwrap())
  }

  fn unlock(&self, guard: Self::LockGuard<'_, T>) {
    drop(guard);
  }
}

pub struct MCSLockGuard<'a, T: ?Sized + 'a> {
  lock: &'a MCSLock<T>,
  node: NonNull<MCSNode>,
}

impl<T: ?Sized> !Send for MCSLockGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for MCSLockGuard<'_, T>{}

impl<'a, T: ?Sized> MCSLockGuard<'a, T> {
  fn new(lock: &'a MCSLock<T>, node: NonNull<MCSNode>) -> Self {
    Self { lock, node }
  }
}

impl<T: ?Sized> Drop for MCSLockGuard<'_, T> {
  fn drop(&mut self) {
    let token = self.node;
    unsafe { 
      if token.as_ref().next.is_null() {
        match self.lock.tail.compare_exchange_weak(
          token.as_ptr(),
          core::ptr::null_mut(),
          Ordering::Release,
          Ordering::Acquire,
        ) {
          Ok(_) => return,
          Err(_) => {},
        }

        while token.as_ref().next.is_null() {}
      }
      (&mut *token.as_ref().next).locked = false;
      (&mut *token.as_ptr()).next = core::ptr::null_mut();
    }
    // TODO: drop node
  }
}

impl<T: ?Sized> Deref for MCSLockGuard<'_, T> {
  type Target = T;

  fn deref(&self) -> &T {
    unsafe { &*self.lock.data.get() }
  }
}

impl<T: ?Sized> DerefMut for MCSLockGuard<'_, T> {
  fn deref_mut(&mut self) -> &mut T {
    unsafe { &mut *self.lock.data.get() }
  }
}