use core::sync::atomic::{AtomicUsize, Ordering};
use core::{cell::UnsafeCell, mem::MaybeUninit, ptr};

use super::{Producer, Consumer, FixSizeSPSCQueue};

// TODO: N >= 1, && N = 2^k
pub struct Simple<T, const N: usize> {
  // TODO: aligned
  buffer: [UnsafeCell<MaybeUninit<T>>; N],
  tail: AtomicUsize,
  head: AtomicUsize,
  head_cache: AtomicUsize,
}

impl<T, const N: usize> Simple<T, N> {
  const INIT: UnsafeCell<MaybeUninit<T>> = UnsafeCell::new(MaybeUninit::uninit());

  pub const fn new() -> Self {
    Self {
      tail: AtomicUsize::new(0),
      head: AtomicUsize::new(0),
      head_cache: AtomicUsize::new(0),
      buffer: [Self::INIT; N],
    }
  }

  pub const fn capacity(&self) -> usize {
    N - 1
  }

  pub fn len(&self) -> usize {
    let current_head = self.head.load(Ordering::Relaxed);
    let current_tail = self.tail.load(Ordering::Relaxed);

    current_tail.wrapping_sub(current_head).wrapping_add(N) % N
  }

  fn increment(val: usize) -> usize {
    (val + 1) % N
  }
}

impl<'a: 'b, 'b, T: 'b, const N: usize> 
  FixSizeSPSCQueue<'a, T, N, SimpleProducer<'a, T, N>, SimpleConsumer<'a, T, N>> 
  for Simple<T, N> 
where Self: 'a
{
    fn split(&'a mut self) -> (SimpleProducer<'a, T, N>, SimpleConsumer<'a, T, N>) {
      (
        SimpleProducer{ inner: self },
        SimpleConsumer{ inner: self },
      )
    }
}

pub struct SimpleProducer<'a, T, const N: usize> {
  inner: &'a Simple<T, N>,
}

impl<'a, T, const N: usize> Producer<'a, T, N> for SimpleProducer<'a, T, N> {
  fn enqueue(&mut self, val: T) -> Result<(), T> {
    let current_tail = self.inner.tail.load(Ordering::Relaxed);
    let next_tail = Simple::<T, N>::increment(current_tail);

    if next_tail != self.inner.head.load(Ordering::Acquire) {
      unsafe {
        let cell = self.inner.buffer.get_unchecked(current_tail).get();
        cell.write(MaybeUninit::new(val));
      }
      self.inner.tail.store(next_tail, Ordering::Release);

      Ok(())
    } else {
      Err(val)
    }
  }
}

pub struct SimpleConsumer<'a, T, const N: usize> {
  inner: &'a Simple<T, N>,
}

impl<'a, T, const N: usize> Consumer<'a, T, N> for SimpleConsumer<'a, T, N> {
  fn dequeue(&mut self) -> Option<T> {
    let current_head = self.inner.head.load(Ordering::Relaxed);

    if current_head == self.inner.tail.load(Ordering::Acquire) {
      None
    } else {
      let v = unsafe {
        (self.inner.buffer.get_unchecked(current_head).get() as *const T).read()
      };

      self.inner.head
        .store(Simple::<T, N>::increment(current_head), Ordering::Release);

      Some(v)
    }
  }
}