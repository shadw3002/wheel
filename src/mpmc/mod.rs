use core::sync::atomic::{Ordering, AtomicU64};
use std::sync::Arc;
use std::usize;
use crossbeam_utils::CachePadded;
use crossbeam_epoch::{Atomic, pin, Shared, Owned, Guard};


const RING_SIZE: usize = 65536;
const CACHELINE_SIZE: usize = core::mem::size_of::<CachePadded<bool>>();

#[repr(align(16))]
pub struct Cell {
    idx: AtomicU64,
    data: AtomicU64,
}
const CELL_SIZE: usize = core::mem::size_of::<Cell>();
const BOX_SIZE: usize = core::mem::size_of::<Box<()>>();
const _: () = assert!(CELL_SIZE == 16);
const _: () = assert!(CACHELINE_SIZE % CELL_SIZE == 0);

impl Cell{
    pub fn new(idx: u64, data: u64) -> Self {
        Self {
            idx: AtomicU64::new(idx),
            data: AtomicU64::new(data),
        }
    }
}

type Ring<T: Sized + Send> = PCQRing<T>;

pub struct PCQRing<T: Sized + Send> {
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    next: CachePadded<Atomic<Ring<T>>>,

    cells: [Cell; RING_SIZE],
}

impl<T: Sized + Send> Drop for PCQRing<T> {
    fn drop(&mut self) {
        debug_assert!(self.dequeue().is_none());
    }
}


impl<T: Sized + Send> PCQRing<T> {
    pub fn new() -> Box<Self> {
        use std::alloc::{alloc, dealloc, Layout};

        unsafe {
            let layout = Layout::new::<Self>();
            let ptr = alloc(layout) as *mut Self;
    
            (*ptr).head = CachePadded::new(AtomicU64::new(RING_SIZE as u64));
            (*ptr).tail = CachePadded::new(AtomicU64::new(RING_SIZE as u64));
            (*ptr).next = CachePadded::new(Atomic::null());
            for (i, cell) in (*ptr).cells.iter_mut().enumerate() {
                *cell = Cell::new(i as u64, 0);
            }
    
            Box::from_raw(ptr)
        }        
    }

    #[inline]
    fn close(&self, tail_ticket: u64, force: bool) -> bool {
        if !force {
            return self.tail.compare_exchange(tail_ticket+1, (tail_ticket + 1) | (1u64<<63), Ordering::SeqCst, Ordering::SeqCst).is_ok();
        }
        else {
            return self.tail.fetch_or(1u64 << 63, Ordering::SeqCst) != 0;
        }
    }

    pub fn enqueue(&self, mut entry: Box<T>) -> Result<(), Box<T>> {
        let mut try_close = 0;
        loop {
            let tail_ticket = self.tail.fetch_add(1, Ordering::AcqRel);
            if is_closed(tail_ticket) {
                return Err(entry);
            }

            let cell = &self.cells[remap(tail_ticket)];
            let (cell_ticket, cell_data) = (cell.idx.load(Ordering::Acquire), cell.data.load(Ordering::Acquire));
            if cell_data == 0 && node_index(cell_ticket) <= tail_ticket && (!is_unsafe(cell_ticket) || self.head.load(Ordering::Acquire) <= tail_ticket) {
                let bottom = thread_local_bottom(ThreadId::current());
                if cell.data.compare_exchange(cell_data, bottom, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                    if cell.idx.compare_exchange(cell_ticket, tail_ticket + RING_SIZE as u64, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                        let entry_ptr = Box::into_raw(entry);
                        if cell.data.compare_exchange(bottom, entry_ptr as u64, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                            return Ok(());
                        }
                        entry = unsafe{ Box::from_raw(entry_ptr) };
                    } else {
                        let _ = cell.data.compare_exchange(bottom, 0, Ordering::AcqRel, Ordering::Relaxed);
                    }
                }
            }

            if tail_ticket >= self.head.load(Ordering::SeqCst) + RING_SIZE as u64 {
                try_close += 1;
                if self.close(tail_ticket, try_close > 10) {
                    return Err(entry);
                }
            }
        }
    }

    pub fn dequeue(&self) -> Option<Box<T>> {
        loop {
            let head_ticket = self.head.fetch_add(1, Ordering::AcqRel);
            let cell = &self.cells[remap(head_ticket)];

            let mut r = 0;
            let mut tt = 0;

            loop {
                let (cell_ticket, cell_data) = (cell.idx.load(Ordering::Acquire), cell.data.load(Ordering::Acquire));
                let is_unsafe = is_unsafe(cell_ticket);
                let cell_idx = node_index(cell_ticket);
                if cell_idx > head_ticket + RING_SIZE as u64 {
                    break;
                }

                if cell_data != 0 && !is_bottom(cell_data) {
                    if cell_idx == head_ticket + RING_SIZE as u64 {
                        cell.data.store(0, Ordering::Release);
                        return unsafe{ Some(Box::from_raw(cell_data as *mut _)) };
                    } else {
                        if is_unsafe {
                            if cell.idx.load(Ordering::Acquire) == cell_ticket {
                                break;
                            }
                        } else {
                            if cell.idx.compare_exchange(cell_ticket, set_unsafe(cell_idx), Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                                break;
                            }
                        }
                    }
                } else {
                    if (r & ((1u64 << 8) - 1)) == 0 {
                        tt = self.tail.load(Ordering::Acquire);
                    }
                    
                    let crq_closed = is_closed(tt);
                    let t = tt & ((1u64 << 63) - 1);
                    
                    if is_unsafe || t < head_ticket + 1 || crq_closed || r > 4 * 1024 {
                        if is_bottom(cell_data) && cell.data.compare_exchange(cell_data, 0, Ordering::AcqRel, Ordering::Relaxed).is_err() {
                            continue;
                        }
                        if cell.idx.compare_exchange(cell_ticket, is_unsafe as u64 | (head_ticket + RING_SIZE as u64), Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                            break;
                        }
                    }
                    r += 1;
                }
            }

            if node_index(self.tail.load(Ordering::Acquire)) <= head_ticket + 1 {
                self.fix_state();
                return None;
            }
        }
    }


    #[inline]
    fn fix_state(&self) {
        loop {
            let head = self.head.load(Ordering::SeqCst);
            let tail = self.tail.load(Ordering::SeqCst);
            if self.tail.load(Ordering::Acquire) != tail {
                continue;
            }
            if head > tail {
                let tmp = tail;
                if self.tail.compare_exchange(tmp, head, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                    break;
                }
                continue;
            }
            break;
        }
    }

    fn is_close(&self) -> bool {
        return (self.tail.load(Ordering::SeqCst) & (1 << 63)) != 0;
    }
}

pub struct UnboundedMPMCQueue<T: Sized + Send> {
    first: CachePadded<Atomic<Ring<T>>>,
    head: CachePadded<Atomic<Ring<T>>>,
    tail: CachePadded<Atomic<Ring<T>>>,
}

impl<T: Sized + Send> UnboundedMPMCQueue<T> {
    pub fn new() -> Self {
        let queue = Self {
            first: CachePadded::new(Atomic::null()),
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
        };
        let guard = &pin();
        let ring = unsafe{ Owned::from_raw(Box::into_raw(Ring::new())) }.into_shared(guard);
        queue.first.store(ring, Ordering::SeqCst);
        queue.head.store(ring, Ordering::SeqCst);
        queue.tail.store(ring, Ordering::SeqCst);

        queue
    }

    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let inner = Arc::new(self);
        (
            Producer{inner: inner.clone()},
            Consumer{inner},
        )
    }

    pub fn check(&self) {
        // loop {
        //     let guard = &pin();
        //     let now = self.first.load(Ordering::SeqCst, guard);
        //     if now.is_null() {
        //         break;
        //     }
        //     let ring = unsafe{ now.as_ref().unwrap() };
        //     if ring.data.load(Ordering::SeqCst) != null_mut() {
        //         panic!("unexpected");
        //     }
        //     self.first.store(ring.next.load(Ordering::SeqCst, guard), Ordering::SeqCst);
        // }
    }
}

pub struct Producer<T: Sized + Send> {
    inner: Arc<UnboundedMPMCQueue<T>>,
}

unsafe impl<T: Send> Send for Producer<T> {}

impl<T: Sized + Send> Producer<T> {
    pub fn enqueue(&self, mut entry: Box<T>) {
        let guard = &pin();

        let mut ring_ptr = self.inner.tail.load(Ordering::Acquire, &guard);
        loop {
            let ring = unsafe{ ring_ptr.as_ref().unwrap_unchecked() };
            
            let next_ring_ptr = ring.next.load(Ordering::Acquire, &guard);
            if !next_ring_ptr.is_null() {
			    // help move cq.next into tail.
                ring_ptr = match self.inner.tail.compare_exchange(
                    ring_ptr, 
                    next_ring_ptr, 
                    Ordering::AcqRel, 
                    Ordering::Acquire,
                    &guard,
                ) {
                    Ok(_) => next_ring_ptr,
                    Err(err) => err.current,
                };
                
			    continue
            }

            entry = match ring.enqueue(entry) {
                Ok(_) => {
                    return
                },
                Err(entry) => entry, // concurrent cq is full
            };

            let new_ring_ptr = Self::new_ring_from_guard(guard);
            let new_ring = unsafe{ new_ring_ptr.as_ref().unwrap_unchecked() };
            let _res = new_ring.enqueue(entry).is_ok();
            debug_assert!(_res);
            match ring.next.compare_exchange(
                Shared::null(), 
                new_ring_ptr, 
                Ordering::AcqRel, 
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => {
                    let _ = self.inner.tail.compare_exchange(
                        ring_ptr, 
                        new_ring_ptr, 
                        Ordering::AcqRel, 
                        Ordering::Relaxed,
                        &guard,
                    );
                    return;
                },
                Err(err) => {
                    ring_ptr = err.current;
                },
            }
            entry = unsafe{ new_ring.dequeue().unwrap_unchecked() };
        }
    }

    #[inline(never)]
    fn new_ring_from_guard<'g>(guard: &'g Guard) -> Shared<'g, PCQRing<T>> {
        unsafe{ Owned::from_raw(Box::into_raw(Ring::new())) }.into_shared(&guard)
    }
}

pub struct Consumer<T: Sized + Send> {
    pub inner: Arc<UnboundedMPMCQueue<T>>,
}

unsafe impl<T: Send> Send for Consumer<T> {}

impl<T: Sized + Send> Consumer<T> {
    pub fn dequeue(&self) -> Option<Box<T>> {
        let guard = pin();

        let mut ring_ptr = self.inner.head.load(Ordering::Acquire, &guard);
        loop {
            let ring = unsafe{ ring_ptr.as_ref().unwrap_unchecked() };

            if let Some(entry) = ring.dequeue() {
                return Some(entry);
            }

            let next_ring_ptr = ring.next.load(Ordering::Acquire, &guard);
            if next_ring_ptr.is_null() {
                return None;
            }

            if let Some(entry) = ring.dequeue() {
                return Some(entry);
            }
 
            ring_ptr = match self.inner.head.compare_exchange(
                ring_ptr, 
                next_ring_ptr, 
                Ordering::AcqRel, 
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => {next_ring_ptr},
                Err(err) => err.new,
            }; 
        }
        
    }
}

#[inline]
fn is_closed(v: u64) -> bool {
    return (v & (1u64 << 63)) != 0;
}

#[inline]
fn is_unsafe(v: u64) -> bool {
    return (v & (1u64 << 63)) != 0;
}

#[inline]
fn set_unsafe(v: u64) -> u64 {
    return v | (1u64 << 63);
}

#[inline]
fn node_index(v: u64) -> u64 {
    return v & ((1u64 << 63) - 1);
}

#[inline]
fn thread_local_bottom(tid: u64) -> u64 {
    (tid << 1) | 1
}

#[inline]
fn is_bottom(v: u64) -> bool {
    (v & 1) != 0
}

#[inline]
fn remap(origin: u64) -> usize {
    const RING_CELLS_SIZE: usize = RING_SIZE * core::mem::size_of::<Cell>();
    const NUM_CACHELINE_PER_RING_CELLS: usize = RING_CELLS_SIZE / CACHELINE_SIZE;
    const NUM_CELLS_PER_CACHELINE: usize = CACHELINE_SIZE / core::mem::size_of::<Cell>();
    let inner = origin as usize % RING_SIZE;
    return inner;
    let cacheline_inner_idx = inner / NUM_CACHELINE_PER_RING_CELLS;
    let cacheline_idx = inner % NUM_CACHELINE_PER_RING_CELLS;
    cacheline_idx * NUM_CELLS_PER_CACHELINE + cacheline_inner_idx
}



use std::thread;
use core::cell;

struct ThreadId {

}

impl ThreadId {
    #[inline]
    pub fn current() -> u64 {
        #[thread_local]
        static CACHED_ID: cell::Cell<u64> = cell::Cell::new(0);
        // Try to have only one TLS read, even though LLVM will
        // definitely emit two for the initial read
        let cached = &CACHED_ID;
        let id = cached.get();

        if core::intrinsics::likely(id != 0) {
            id
        } else {
            Self::get_and_cache(cached)
        }
    }

    #[cold]
    #[inline(never)]
    fn get_and_cache(cache: &cell::Cell<u64>) -> u64 {
        let id = thread::current().id().as_u64().get();
        cache.set(id);
        id
    }
}

