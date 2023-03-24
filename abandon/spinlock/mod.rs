mod taslock;
mod ticketlock;
mod clhlock;
mod mcslock;

pub trait Mutex<T: ?Sized> {
  type LockGuard<'a, V: ?Sized + 'a> where Self: 'a;
  
  fn lock(&self) -> Self::LockGuard<'_, T>;
  fn unlock(&self, guard: Self::LockGuard<'_, T>);
}
