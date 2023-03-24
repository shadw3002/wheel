#![feature(integer_atomics)]

use wheel::mpmc::{UnboundedMPMCQueue, Cell};
use std::thread;

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
            });
        }

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

fn mpmc() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();

    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for i in 0..MESSAGES / THREADS {
                    producer.enqueue(Box::new(message::new(i)));
                }
            });
        }

        for _ in 0..THREADS {
            scope.spawn(|_| {
                for i in 0..MESSAGES / THREADS {
                    loop {
                        if consumer.dequeue().is_none() {
                            thread::yield_now();
                        } else {
                            break;
                        }
                    }
                }
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

    run!("unbounded_seq", seq());
    run!("unbounded_spsc", spsc());
    run!("unbounded_mpsc", mpsc());
    run!("unbounded_mpmc", mpmc());
}
