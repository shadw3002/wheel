#![feature(integer_atomics)]

use wheel::mpmc::{UnboundedMPMCQueue, PCQRing};
use std::{thread, time, cmp::Ordering};

mod message;

const MESSAGES: usize = 5_000_0;
const THREADS: usize = 4;

fn test() {
    let ring = PCQRing::new();

    for i in 0..MESSAGES {
        ring.enqueue(Box::new(message::new(i)));
    }

    for i in 0..MESSAGES {
        ring.dequeue().unwrap();
    }
}

fn seq() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();

    for i in 0..MESSAGES {
        producer.enqueue(Box::new(message::new(i)));
    }

    for i in 0..MESSAGES {
        consumer.dequeue().unwrap();
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

        for i in 0..MESSAGES {
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

fn spmc() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();
    let (producer, consumer) = (&producer, &consumer);

    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for i in 0..MESSAGES {
                producer.enqueue(Box::new(message::new(i)));
            }

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
                                // println!("thread: {}: miss: {}", t, i);
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
                // println!("write done");
            });
        }

        for t in 0..THREADS {
            scope.spawn(move |_| {
                for i in 0..MESSAGES / THREADS {
                    loop {
                        match consumer.dequeue() {
                            Some(entry) => {
                                break;
                            },
                            None => {             
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


    // run!("unbounded_mpmc", mpmc());
    // run!("unbounded_mpsc", mpsc());
    run!("unbounded_seq", seq());
    // run!("unbounded_spsc", spsc());
}
