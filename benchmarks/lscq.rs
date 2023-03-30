#![feature(integer_atomics)]

use wheel::mpmc::{UnboundedMPMCQueue, Cell};
use std::{thread, time, cmp::Ordering};

mod message;

const MESSAGES: usize = 5_000_000;
const THREADS: usize = 4;

fn seq() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();

    for i in 0..MESSAGES {
        producer.enqueue(Box::new(message::new(i)));
    }

    for i in 0..MESSAGES {
        let j = consumer.dequeue().unwrap().as_ref().0[0];
        assert_eq!(i, j);
    }
}

fn spsc() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();

    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for i in 0..MESSAGES {
                producer.enqueue(Box::new(message::new(i)));
            }
        });

        for _ in 0..MESSAGES {
            loop {
                if consumer.dequeue().is_none() {
                    thread::yield_now();
                } else {
                    break;
                }
            }
        }
    })
    .unwrap();
}

fn mpsc() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for i in 0..MESSAGES / THREADS {
                    producer.enqueue(Box::new(message::new(i)));
                }

                println!("write done");
            });
        }

        for i in 0..MESSAGES {
            loop {
                if consumer.dequeue().is_none() {
                    thread::yield_now();
                    println!("miss: {}", i);
                } else {
                    break;
                }
            }
        }
    })
    .unwrap();
}

fn spmc() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();
    let (producer, consumer) = (&producer, &consumer);

    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for i in 0..MESSAGES {
                producer.enqueue(Box::new(message::new(i)));
            }

            println!("write done");
        });

        for t in 0..THREADS {
            scope.spawn(move |_| {
                for i in 0..MESSAGES / THREADS {
                    loop {
                        match consumer.dequeue() {
                            Some(entry) => {
                                break;
                            },
                            None => {
                                println!("thread: {}: miss: {}", t, i);
                                thread::yield_now();
                            },
                        }
                    }
                }
            });
        }
    })
    .unwrap();
}

fn mpmc() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();
    let (producer, consumer) = (&producer, &consumer);

    crossbeam::scope(|scope| {
        for t in 0..THREADS {
            scope.spawn(move |_| {
                for i in 0..MESSAGES / THREADS {
                    // println!("thread: {}: producing: {}", t, i);
                    producer.enqueue(Box::new(message::new(i)));
                }
                println!("write done");
            });
        }

        for t in 0..THREADS {
            scope.spawn(move |_| {
                println!("start reading");
                for i in 0..MESSAGES / THREADS {
                    loop {
                        match consumer.dequeue() {
                            Some(entry) => {
                                break;
                            },
                            None => {
                                let enqs = consumer.inner.enqs.load(core::sync::atomic::Ordering::Acquire);
                                let deqs = consumer.inner.deqs.load(core::sync::atomic::Ordering::Acquire);
                                let new_rings = consumer.inner.new_rings.load(core::sync::atomic::Ordering::Acquire);
                                let drop_rings = consumer.inner.drop_rings.load(core::sync::atomic::Ordering::Acquire);
                                
                                println!("thread: {}: miss: {}, enqs: {}, deqs: {}, new_rings: {}, drop_rings: {}", t, i, enqs, deqs, new_rings, drop_rings);
                                thread::yield_now();
                            },
                        }
                    }
                }
                thread::sleep(time::Duration::from_secs(20));
                println!("checking");
                consumer.inner.check();
            });
        }
    })
    .unwrap();
}

fn main() {
    macro_rules! run {
        ($name:expr, $f:expr) => {
            let now = ::std::time::Instant::now();
            $f;
            let elapsed = now.elapsed();
            println!(
                "{:25} {:15} {:7.3} sec",
                $name,
                "Rust segqueue",
                elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9
            );
        };
    }

    // run!("unbounded_seq", seq());
    // run!("unbounded_spsc", spsc());
    // run!("unbounded_mpsc", spmc());
    // run!("unbounded_mpsc", mpsc());
    run!("unbounded_mpmc", mpmc());
}
