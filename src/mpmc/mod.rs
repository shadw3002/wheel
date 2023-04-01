use core::sync::atomic::{Ordering, AtomicPtr, AtomicU64, AtomicI64, AtomicU128};
use std::sync::Mutex;
use std::ptr::null_mut;
use std::sync::Arc;
use std::usize;
use crossbeam_utils::CachePadded;
use crossbeam_epoch::{Atomic, pin, Shared, Owned};


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
        let flags = self.flags.load(Ordering::SeqCst);
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
            flags: AtomicU64::new(cell.flags.load(Ordering::SeqCst)), 
            data: AtomicU64::new(cell.data.load(Ordering::SeqCst)),
        }
    }

    pub fn reset(&self) {
        self.data.store(0, Ordering::SeqCst);
        self.flags.store(Self::new_flags(true, true, 0), Ordering::SeqCst);
    }
}

type Ring<T: Sized + Send> = RingSimple<T>;

struct RingSimple<T: Sized + Send> {
    mutex: Mutex<()>,
    data: CachePadded<AtomicPtr<T>>,
    next: CachePadded<Atomic<Ring<T>>>,
    tail: CachePadded<AtomicU64>,
    threshold: CachePadded<AtomicI64>,
}

impl<T: Sized + Send> RingSimple<T> {
    pub fn new() -> Self {
        // println!("newing simple ring");
        Self { 
            mutex: Mutex::new(()),
            data: CachePadded::new(AtomicPtr::new(null_mut())),
            next: CachePadded::new(Atomic::null()), 
            tail: CachePadded::new(AtomicU64::new(0)),
            threshold: CachePadded::new(AtomicI64::new(0)),
        }
    }

    pub fn enqueue(&self, entry: Box<T>) -> Result<(), Box<T>> {
        let guard = self.mutex.lock().unwrap();
        if self.is_close() {
            return Err(entry);
        }
        let entry_ptr = Box::into_raw(entry);
        match self.data.compare_exchange(null_mut(), entry_ptr, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => Ok(()),
            Err(_) => unsafe{ Err(Box::from_raw(entry_ptr)) },
        }
    }

    pub fn dequeue(&self) -> Option<Box<T>> {
        let entry_ptr = self.data.load(Ordering::SeqCst);
        if entry_ptr == null_mut() {
            return None;
        }
        match self.data.compare_exchange(entry_ptr, null_mut(), Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => Some(unsafe{ Box::from_raw(entry_ptr) }),
            Err(_) => None,
        }
    }

    pub fn close(&self) {
        self.tail.fetch_or(1 << 63, Ordering::SeqCst);
    }

    fn is_close(&self) -> bool {
        return (self.tail.load(Ordering::SeqCst) & (1 << 63)) != 0;
    }
}


impl<T: Sized + Send> Drop for RingSimple<T> {
    fn drop(&mut self) {
        println!("dropping simple ring");
        debug_assert_eq!(self.data.load(Ordering::SeqCst), null_mut());
    }
}

struct SCQRing<T: Sized + Send> {
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
    threshold: CachePadded<AtomicI64>,
    next: CachePadded<Atomic<Ring<T>>>,

    cells: [Cell; RING_SIZE],
}

impl<T: Sized + Send> Drop for SCQRing<T> {
    fn drop(&mut self) {
        debug_assert!(self.dequeue().is_none());
    }
}


impl<T: Sized + Send> SCQRing<T> {
    pub fn new() -> Self {
        Self { 
            head: CachePadded::new(AtomicU64::new(RING_SIZE as u64)), 
            tail: CachePadded::new(AtomicU64::new(RING_SIZE as u64)), 
            threshold: CachePadded::new(AtomicI64::new(-1)), 
            next: CachePadded::new(Atomic::null()), 
            cells: [(); RING_SIZE].map(|_| Cell::new(Cell::new_flags(true, true, 0), 0)),
        }
    }

    pub fn enqueue(&self, mut entry: Box<T>) -> Result<(), Box<T>> {
        loop {
            let raw_tail = self.tail.fetch_add(1, Ordering::AcqRel);
            let tail = raw_tail & ((1 << 63) - 1);
            let is_closed = (raw_tail & (1 << 63)) != 0;
            if is_closed {
                return Err(entry)
            }

            let idx = Self::remap(tail);
            let cell: &Cell = &self.cells[idx];
            let tail_cycle = tail / RING_SIZE as u64;
            
            loop {
                // enqueue do not need data, if this entry is empty, we can assume the data is also empty.
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
                    // debug_assert_eq!(cell.data.load(Ordering::SeqCst), 0);
                    if cell.as_u128().compare_exchange(
                        old.as_u128().load(Ordering::Acquire), 
                        new.as_u128().load(Ordering::Acquire), 
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
        if self.threshold.load(Ordering::SeqCst) < 0 {
            // Empty queue.
            return None
        }

        loop {
            let raw_head = self.head.fetch_add(1, Ordering::SeqCst);
            let head = raw_head & ((1 << 63) - 1);        
            let cell_ref: &Cell = &self.cells[Self::remap(head)];
            let head_cycle = head / RING_SIZE as u64;

            loop {
                let cell = Cell::from_u128(cell_ref.as_u128().load(Ordering::SeqCst));
                let (is_safe, is_empty, cell_cycle, _) = cell.flags();
                if cell_cycle == head_cycle {
                    // clear data in this slot
                    cell_ref.reset();
                    return unsafe {
                        Some(Box::from_raw(cell.data.load(Ordering::SeqCst) as *mut T))
                    };
                }
                if cell_cycle < head_cycle {
                    let new_cell = match is_empty {
                        true => Cell::new(Cell::new_flags(is_safe, true, head_cycle), 0),
                        false => Cell::new(Cell::new_flags(false, false, cell_cycle), cell.data.load(Ordering::SeqCst)),
                    };
                    if cell_ref.as_u128().compare_exchange(
                        cell.as_u128().load(Ordering::SeqCst), 
                        new_cell.as_u128().load(Ordering::SeqCst), 
                        Ordering::SeqCst, 
                        Ordering::SeqCst
                    ).is_err() {
                        continue;
                    }
                }
                break;
            }

            let raw_tail = self.tail.load(Ordering::SeqCst);
            let tail = raw_tail & ((1 << 63) - 1);  
            if tail < head + 1 {
                // invalid state
                self.fix_state(head + 1);
                self.threshold.fetch_add(-1, Ordering::SeqCst);
                return None;
            }
            if self.threshold.fetch_add(-1, Ordering::SeqCst) <= 0 {
                return None;
            }
        }
    }

    fn remap(origin: u64) -> usize {
        let inner = origin as usize % RING_SIZE;
        return inner;
        const RING_CELLS_SIZE: usize = RING_SIZE * core::mem::size_of::<Cell>();
        const NUM_CACHELINE_PER_RING_CELLS: usize = RING_CELLS_SIZE / CACHELINE_SIZE;
        const NUM_CELLS_PER_CACHELINE: usize = CACHELINE_SIZE / core::mem::size_of::<Cell>();
        let cacheline_inner_idx = inner / NUM_CACHELINE_PER_RING_CELLS;
        let cacheline_idx = inner % NUM_CACHELINE_PER_RING_CELLS;
        cacheline_idx * NUM_CELLS_PER_CACHELINE + cacheline_inner_idx
    }

    fn fix_state(&self, original_head: u64) {
        loop {
            let head = self.head.load(Ordering::SeqCst);
            if original_head < head {
                // The last dequeuer will be responsible for fixstate.
                return
            }
            let tail = self.tail.load(Ordering::SeqCst);
            if tail >= head {
                // The queue has been closed, or in normal state.
                return 
            }
            if self.tail.compare_exchange(tail, head, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                return
            }
        }
    }

    pub fn close(&self) {
        self.tail.fetch_or(1 << 63, Ordering::SeqCst);
    }

    fn is_close(&self) -> bool {
        return (self.tail.load(Ordering::SeqCst) & (1 << 63)) != 0;
    }
}

pub struct UnboundedMPMCQueue<T: Sized + Send> {
    first: CachePadded<Atomic<Ring<T>>>,
    head: CachePadded<Atomic<Ring<T>>>,
    tail: CachePadded<Atomic<Ring<T>>>,
    pub enqs: AtomicI64,
    pub deqs: AtomicI64,
    pub new_rings: AtomicI64,
    pub drop_rings: AtomicI64,
}

impl<T: Sized + Send> UnboundedMPMCQueue<T> {
    pub fn new() -> Self {
        let queue = Self {
            first: CachePadded::new(Atomic::null()),
            head: CachePadded::new(Atomic::null()),
            tail: CachePadded::new(Atomic::null()),
            enqs: AtomicI64::new(0),
            deqs: AtomicI64::new(0),
            new_rings: AtomicI64::new(0),
            drop_rings: AtomicI64::new(0),
        };
        let guard = &pin();
        let ring = Owned::new(Ring::new()).into_shared(guard);
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
        loop {
            let guard = &pin();
            let now = self.first.load(Ordering::SeqCst, guard);
            if now.is_null() {
                break;
            }
            let ring = unsafe{ now.as_ref().unwrap() };
            if ring.data.load(Ordering::SeqCst) != null_mut() {
                panic!("unexpected");
            }
            self.first.store(ring.next.load(Ordering::SeqCst, guard), Ordering::SeqCst);
        }
    }
}

pub struct Producer<T: Sized + Send> {
    inner: Arc<UnboundedMPMCQueue<T>>,
}

unsafe impl<T: Send> Send for Producer<T> {}

impl<T: Sized + Send> Producer<T> {
    pub fn enqueue(&self, mut entry: Box<T>) {
        let guard = &pin();
        guard.flush();

        let mut ring_ptr = None;
        loop {
            ring_ptr = Some(*ring_ptr.get_or_insert_with(|| self.inner.tail.load(Ordering::Acquire, &guard)));
            let ring = unsafe{ ring_ptr.unwrap_unchecked().as_ref().unwrap_unchecked() };
            
            let mut next_ring_ptr = ring.next.load(Ordering::Acquire, &guard);
            if !next_ring_ptr.is_null() {
			    // help move cq.next into tail.
                ring_ptr = Some(match self.inner.tail.compare_exchange(
                    unsafe{ ring_ptr.unwrap_unchecked() }, 
                    next_ring_ptr, 
                    Ordering::AcqRel, 
                    Ordering::Acquire,
                    &guard,
                ) {
                    Ok(_) => next_ring_ptr,
                    Err(err) => err.current,
                });
                ring_ptr = None;
                
			    continue
            }

            entry = match ring.enqueue(entry) {
                Ok(_) => {
                    self.inner.enqs.fetch_add(1, Ordering::AcqRel);
                    return
                },
                Err(entry) => entry, // concurrent cq is full
            };
            // close cq, subsequent enqueue will fail
            ring.close();

            next_ring_ptr = ring.next.load(Ordering::Acquire, &guard);
            if !next_ring_ptr.is_null() {
                ring_ptr = Some(next_ring_ptr);
                ring_ptr = None;
                continue;
            }

            let new_ring_ptr = Owned::new(Ring::new()).into_shared(&guard);
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
                        unsafe{ ring_ptr.unwrap_unchecked() }, 
                        new_ring_ptr, 
                        Ordering::AcqRel, 
                        Ordering::Relaxed,
                        &guard,
                    );
                    self.inner.enqs.fetch_add(1, Ordering::AcqRel);
                    self.inner.new_rings.fetch_add(1, Ordering::AcqRel);
                    return;
                },
                Err(err) => {
                    ring_ptr = Some(err.current);
                },
            }
            entry = unsafe{ new_ring.dequeue().unwrap_unchecked() };
        }
    }
}

pub struct Consumer<T: Sized + Send> {
    pub inner: Arc<UnboundedMPMCQueue<T>>,
}

unsafe impl<T: Send> Send for Consumer<T> {}

impl<T: Sized + Send> Consumer<T> {
    pub fn dequeue(&self) -> Option<Box<T>> {
        let guard = pin();
        guard.flush();

        let mut ring_ptr = None;
        loop {
            ring_ptr = Some(*ring_ptr.get_or_insert_with(|| self.inner.head.load(Ordering::Acquire, &guard)));
            let ring = unsafe{ ring_ptr.unwrap_unchecked().as_ref().unwrap_unchecked() };

            if let Some(entry) = ring.dequeue() {
                self.inner.deqs.fetch_add(1, Ordering::AcqRel);
                return Some(entry);
            }

            let next_ring_ptr = ring.next.load(Ordering::Acquire, &guard);
            if next_ring_ptr.is_null() {
                return None;
            }

            // cq.next is not empty, subsequent entry will be insert into cq.next instead of cq.
		    // So if cq is empty, we can move it into ncqpool.
            ring.threshold.store((RING_SIZE as i64) * 2 - 1, Ordering::Release);
            {
                let guard = ring.mutex.lock().unwrap();
                ring.close();
                if let Some(entry) = ring.dequeue() {
                    self.inner.deqs.fetch_add(1, Ordering::AcqRel);
                    return Some(entry);
                }
            }
 
            ring_ptr = Some(match self.inner.head.compare_exchange(
                unsafe{ ring_ptr.unwrap_unchecked() }, 
                next_ring_ptr, 
                Ordering::AcqRel, 
                Ordering::Acquire,
                &guard,
            ) {
                Ok(_) => {self.inner.drop_rings.fetch_add(1, Ordering::AcqRel); next_ring_ptr},
                Err(err) => err.new,
            }); 
        }
        
    }
}