use core::sync::atomic::{Ordering, AtomicPtr, AtomicU64, AtomicI64, AtomicU128};
use std::ptr::null_mut;
use std::sync::Arc;
use std::usize;
use crossbeam_utils::CachePadded;

const RING_SIZE: usize = 128; // 65536;
const CACHELINE_SIZE: usize = core::mem::size_of::<CachePadded<bool>>();

#[repr(align(16))]
pub struct Cell {
    flags: AtomicU64,
    data: AtomicU64,
}
const CELL_SIZE: usize = core::mem::size_of::<Cell>();
const _: () = assert!(CELL_SIZE == 16);
const _: () = assert!(CACHELINE_SIZE % CELL_SIZE == 0);

impl Cell{
    pub fn new(flags: u64, data: u64) -> Self {
        Self {
            flags: AtomicU64::new(flags),
            data: AtomicU64::new(data),
        }
    }

    pub fn new_flags(is_safe: bool, is_empty: bool, cell_cycle: u64) -> u64 {
        let mut v = cell_cycle & ((1 << 62) - 1);
        if is_safe {
            v |= 1 << 63
        }
        if is_empty {
            v |= 1 << 62
        }
        v
    }

    pub fn flags(&self) -> (bool, bool, u64, u64) {
        let flags = self.flags.load(Ordering::Acquire);
        let is_safe = (flags & (1 << 63)) != 0;
        let is_empty = (flags & (1 << 62)) != 0;
        let cycle = flags & ((1 << 62) - 1);
        (is_safe, is_empty, cycle, flags)
    }

    pub fn as_u128<'a>(&'a self) -> &'a AtomicU128 {
        unsafe{ &*(self as *const Cell as *const AtomicU128) }
    }

    pub fn from_u128(mut raw: u128) -> Self {
        let cell = unsafe{  &*(&mut raw as *mut u128 as *mut Cell) };
        Self { 
            flags: AtomicU64::new(cell.flags.load(Ordering::Relaxed)), 
            data: AtomicU64::new(cell.data.load(Ordering::Relaxed)),
        }
    }

    pub fn reset(&self) {
        self.data.store(0, Ordering::Release);
        self.flags.store(Self::new_flags(true, true, 0), Ordering::Release);
    }
}

struct Ring<T: Sized + Send> {
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    threshold: CachePadded<AtomicI64>,
    next: CachePadded<AtomicPtr<Ring<T>>>,

    cells: [Cell; RING_SIZE],
}


impl<T: Sized + Send> Ring<T> {
    pub fn new() -> Self {
        Self { 
            head: CachePadded::new(AtomicU64::new(RING_SIZE as u64)), 
            tail: CachePadded::new(AtomicU64::new(RING_SIZE as u64)), 
            threshold: CachePadded::new(AtomicI64::new(-1)), 
            next: CachePadded::new(AtomicPtr::new(null_mut())), 
            cells: [(); RING_SIZE].map(|_| Cell::new(Cell::new_flags(true, true, 0), 0)),
        }
    }

    pub fn enqueue(&self, mut entry: Box<T>) -> Result<(), Box<T>> {
        loop {
            let raw_tail = self.tail.fetch_add(1, Ordering::AcqRel);
            let tail = raw_tail & ((1 << 63) - 1);
            let is_closed = (raw_tail & (1 << 63)) == (1 << 63);
            if is_closed {
                return Err(entry)
            }

            let idx = Self::remap(tail);
            let cell: &Cell = &self.cells[idx];
            let tail_cycle = tail / RING_SIZE as u64;
            
            loop {
                let (is_safe, is_empty, cell_cycle, flags) = cell.flags();
                if cell_cycle < tail_cycle && is_empty && (is_safe ||self.head.load(Ordering::Acquire) <= tail) {
                    // We can use this entry for adding new data if
			        // 1. Tail's cycle is bigger than entry's cycle.
			        // 2. It is empty.
			        // 3. It is safe or tail >= head (There is enough space for this data)

                    let entry_ptr = Box::into_raw(entry);
                    let old = Cell::new(flags, 0);
                    let new = Cell::new(Cell::new_flags(true, false, tail_cycle), entry_ptr as u64);
                    // Save input data into this entry.
                    debug_assert_eq!(cell.data.load(Ordering::Acquire), 0);
                    if cell.as_u128().compare_exchange(
                        old.as_u128().load(Ordering::Relaxed), 
                        new.as_u128().load(Ordering::Relaxed), 
                        Ordering::AcqRel, 
                        Ordering::Relaxed,
                    ).is_err() {
                        entry = unsafe{ Box::from_raw(entry_ptr) };
                        continue;
                    }

                    // Success.
			        if self.threshold.load(Ordering::Acquire) != (RING_SIZE as i64) * 2 - 1 {
                        self.threshold.store((RING_SIZE as i64) * 2 - 1, Ordering::Release);
			        }

			        return Ok(());
                }
                break;
            }

            // Add a full queue check in the loop(CAS2).
            if tail + 1 >= self.head.load(Ordering::Acquire) + RING_SIZE as u64 {
                // T is tail's value before FAA(1), latest tail is T+1.
                return Err(entry);
            }
        }
        
    }

    pub fn dequeue(&self) -> Option<Box<T>> {
        if self.threshold.load(Ordering::Acquire) < 0 {
            // Empty queue.
            return None
        }

        loop {
            let raw_head = self.head.fetch_add(1, Ordering::AcqRel);
            let head = raw_head & ((1 << 63) - 1);        
            let cell_ref: &Cell = &self.cells[Self::remap(head)];
            let head_cycle = head / RING_SIZE as u64;

            loop {
                let cell = Cell::from_u128(cell_ref.as_u128().load(Ordering::Acquire));
                let (is_safe, is_empty, cell_cycle, _) = cell.flags();
                if cell_cycle == head_cycle {
                    // clear data in this slot
                    cell_ref.reset();
                    return unsafe {
                        Some(Box::from_raw(cell.data.load(Ordering::Relaxed) as *mut T))
                    };
                }
                if cell_cycle < head_cycle {
                    let new_cell = match is_empty {
                        true => Cell::new(Cell::new_flags(is_safe, true, head_cycle), 0),
                        false => Cell::new(Cell::new_flags(false, false, cell_cycle), cell.data.load(Ordering::Relaxed)),
                    };
                    if cell_ref.as_u128().compare_exchange(
                        cell.as_u128().load(Ordering::Relaxed), 
                        new_cell.as_u128().load(Ordering::Relaxed), 
                        Ordering::AcqRel, 
                        Ordering::Relaxed
                    ).is_err() {
                        continue;
                    }
                }
                break;
            }

            let raw_tail = self.tail.load(Ordering::Acquire);
            let tail = raw_tail & ((1 << 63) - 1);  
            if tail < head + 1 {
                // invalid state
                self.fix_state(head + 1);
                self.threshold.fetch_add(-1, Ordering::AcqRel);
                return None;
            }
            if self.threshold.fetch_add(-1, Ordering::AcqRel) <= 0 {
                return None;
            }
        }
    }

    fn remap(origin: u64) -> usize {
        let inner = origin as usize % RING_SIZE;
        const RING_CELLS_SIZE: usize = RING_SIZE * core::mem::size_of::<Cell>();
        const NUM_CACHELINE_PER_RING_CELLS: usize = RING_CELLS_SIZE / CACHELINE_SIZE;
        const NUM_CELLS_PER_CACHELINE: usize = CACHELINE_SIZE / core::mem::size_of::<Cell>();
        let cacheline_inner_idx = inner / NUM_CACHELINE_PER_RING_CELLS;
        let cacheline_idx = inner % NUM_CACHELINE_PER_RING_CELLS;
        cacheline_idx * NUM_CELLS_PER_CACHELINE + cacheline_inner_idx
    }

    fn fix_state(&self, original_head: u64) {
        loop {
            let head = self.head.load(Ordering::Acquire);
            if original_head < head {
                // The last dequeuer will be responsible for fixstate.
                return
            }
            let tail = self.tail.load(Ordering::Acquire);
            if tail >= head {
                // The queue has been closed, or in normal state.
                return 
            }
            if self.tail.compare_exchange(tail, head, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return
            }
        }
    }
}

pub struct UnboundedMPMCQueue<T: Sized + Send> {
    head: AtomicPtr<Ring<T>>,
    tail: AtomicPtr<Ring<T>>,
}

impl<T: Sized + Send> UnboundedMPMCQueue<T> {
    pub fn new() -> Self {
        let ring_ptr = Box::into_raw(Box::new(Ring::new()));
        let res = Self {
            head: AtomicPtr::new(ring_ptr),
            tail: AtomicPtr::new(ring_ptr),
        };
        res
    }

    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let inner = Arc::new(self);
        (
            Producer{inner: inner.clone()},
            Consumer{inner},
        )
    }
}

pub struct Producer<T: Sized + Send> {
    inner: Arc<UnboundedMPMCQueue<T>>,
}

unsafe impl<T: Send> Send for Producer<T> {}

impl<T: Sized + Send> Producer<T> {
    pub fn enqueue(&self, mut entry: Box<T>) {
        let mut ring_ptr = None;
        loop {
            if ring_ptr.is_none() {
                ring_ptr = Some(self.inner.tail.load(Ordering::Acquire));
            }
            let ring = unsafe{&*(ring_ptr.unwrap_unchecked() as *const Ring<T>)};
            let mut next_ring_ptr = ring.next.load(Ordering::Acquire);
            if next_ring_ptr != null_mut() {
			    // help move cq.next into tail.
                ring_ptr = match self.inner.tail.compare_exchange(
                    unsafe{ ring_ptr.unwrap_unchecked() }, 
                    next_ring_ptr, 
                    Ordering::AcqRel, 
                    Ordering::Acquire,
                ) {
                    Ok(_) => Some(next_ring_ptr),
                    Err(new_tail) => Some(new_tail),
                };
                
			    continue
            }

            entry = match ring.enqueue(entry) {
                Ok(_) => return,
                Err(entry) => entry, // concurrent cq is full
            };
            // close cq, subsequent enqueue will fail
            ring.tail.fetch_or(1 << 63, Ordering::Release);

            next_ring_ptr = ring.next.load(Ordering::Acquire);
            if next_ring_ptr!= null_mut() {
                ring_ptr = Some(next_ring_ptr);
                continue;
            }

            let new_ring_ptr = Box::into_raw(Box::new(Ring::<T>::new()));
            let new_ring = unsafe{ Box::from_raw(new_ring_ptr) };
            let _res = new_ring.enqueue(entry).is_ok();
            debug_assert!(_res);
            match ring.next.compare_exchange(
                null_mut(), 
                new_ring_ptr, Ordering::AcqRel, 
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    core::mem::forget(new_ring);

                    debug_assert!(ring_ptr.is_some());
                    let _ = self.inner.tail.compare_exchange(
                        unsafe{ ring_ptr.unwrap_unchecked() }, 
                        new_ring_ptr, 
                        Ordering::AcqRel, 
                        Ordering::Relaxed,
                    );
                    return;
                },
                Err(new_next) => {
                    ring_ptr = Some(new_next);
                }
            }
            entry = unsafe{ new_ring.dequeue().unwrap_unchecked() };
        }
    }
}

pub struct Consumer<T: Sized + Send> {
    inner: Arc<UnboundedMPMCQueue<T>>,
}

unsafe impl<T: Send> Send for Consumer<T> {}

impl<T: Sized + Send> Consumer<T> {
    pub fn dequeue(&self) -> Option<Box<T>> {
        let mut ring_ptr = None;
        loop {
            if ring_ptr.is_none() {
                ring_ptr = Some(self.inner.head.load(Ordering::Acquire));
            }
            let ring = unsafe{ &*(ring_ptr.unwrap_unchecked() as *const Ring<T>) };

            if let Some(entry) = ring.dequeue() {
                return Some(entry);
            }

            let next_ring_ptr = ring.next.load(Ordering::Acquire);
            if next_ring_ptr == null_mut() {
                return None;
            }

            // cq.next is not empty, subsequent entry will be insert into cq.next instead of cq.
		    // So if cq is empty, we can move it into ncqpool.
            ring.threshold.store((RING_SIZE as i64) * 2 - 1, Ordering::Release);
            if let Some(entry) = ring.dequeue() {
                return Some(entry);
            }

            match self.inner.head.compare_exchange(
                unsafe{ ring_ptr.unwrap_unchecked() }, 
                next_ring_ptr, 
                Ordering::AcqRel, 
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    unsafe{ drop(Box::from_raw(ring_ptr.unwrap_unchecked())); }
                    ring_ptr = Some(next_ring_ptr);
                },
                Err(new_head) => {
                    ring_ptr = Some(new_head);
                }
            }
        }
    }
}