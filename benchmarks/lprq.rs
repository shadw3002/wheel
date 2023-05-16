#![feature(integer_atomics)]

use wheel::mpmc::UnboundedMPMCQueue;
use std::thread;

mod message;

const MESSAGES: usize = 5_000_000;
const THREADS: usize = 4;

fn seq() {
    let q = UnboundedMPMCQueue::new();
    let (producer, consumer) = q.split();
    let ptr = Box::into_raw(Box::new(message::new(0)));


    for _i in 0..MESSAGES {
        producer.enqueue(unsafe{ Box::from_raw(ptr) });
    }

    for _i in 0..MESSAGES {
        let b = consumer.dequeue().unwrap();
        Box::into_raw(b);
    }

    unsafe{ let _ = Box::from_raw(ptr); }
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

        for _i in 0..MESSAGES {
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

        for _t in 0..THREADS {
            scope.spawn(move |_| {
                for _i in 0..MESSAGES / THREADS {
                    loop {
                        match consumer.dequeue() {
                            Some(_entry) => {
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
        for _t in 0..THREADS {
            scope.spawn(move |_| {
                for i in 0..MESSAGES / THREADS {
                    // println!("thread: {}: producing: {}", t, i);
                    producer.enqueue(Box::new(message::new(i)));
                }
                // println!("write done");
            });
        }

        for _t in 0..THREADS {
            scope.spawn(move |_| {
                for _i in 0..MESSAGES / THREADS {
                    loop {
                        match consumer.dequeue() {
                            Some(_entry) => {
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


    run!("unbounded_mpmc", mpmc());
    run!("unbounded_mpsc", mpsc());
    run!("unbounded_spmc", spmc());
    run!("unbounded_seq", seq());
    run!("unbounded_spsc", spsc());
}
