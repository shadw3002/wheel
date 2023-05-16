mod simple;

pub trait FixSizeSPSCQueue<'a, T, const N: usize, P, C> where Self: 'a {
  fn split(&'a mut self) -> (P, C) where P: Producer<'a, T, N>, C: Consumer<'a, T, N>;
}

pub trait Producer<'a, T, const N: usize> {
  fn enqueue(&mut self, val: T) -> Result<(), T>;
}

pub trait Consumer<'a, T, const N: usize> {
  fn dequeue(&mut self) -> Option<T>;
}