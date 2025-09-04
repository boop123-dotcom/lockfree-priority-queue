mod mdlist;

#[global_allocator]
static GLOBAL: System = System;

use std::alloc::System;
use crossbeam::epoch::{self, Atomic};
use std::sync::Arc;
use std::thread;
use std::io::Write;
use mdlist::{MDList, Stack};
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::time::{Instant, Duration};

const DIMENSION: usize = 4;
const RANGE: usize = 10000;
const CONCURRENT_OPS: usize = 1000;

fn test_scenario(name: &str, keys: Vec<u32>, values: Vec<u8>) {
    println!("\n=== Testing scenario: {} ===", name);
    std::io::stdout().flush().unwrap();
    
    let pq = MDList::new(DIMENSION, RANGE);
    let guard = &epoch::pin();

    for (key, &val) in keys.iter().zip(values.iter()) {
        pq.insert(*key, Box::into_raw(Box::new(val)) as *mut u8);
    }

    let mut sorted_pairs: Vec<_> = keys.iter().zip(values.iter()).collect();
    sorted_pairs.sort_by_key(|(&k, _)| k);
    let mut expected_order = sorted_pairs.into_iter().map(|(&k, &v)| (k, v)).collect::<Vec<_>>();

    let head_node = pq.head_ptr(guard);
    let temp_stack = Stack {
        head: Atomic::from(head_node),
        del: std::array::from_fn(|_| Atomic::from(head_node)),
    };

    while !expected_order.is_empty() {
        if let Some(min_node) = pq.delete_min(&temp_stack, guard) {
            unsafe {
                let val_ptr = (*min_node.as_raw()).val.load(Ordering::Acquire);
                if !val_ptr.is_null() {
                    let val = *val_ptr;
                    let min_key = (*min_node.as_raw()).key;
                    
                    let (expected_key, expected_val) = expected_order.remove(0);
                    println!("Deleted key: {}, value: {}", expected_key, expected_val);
                    std::io::stdout().flush().unwrap();
                    
                    assert_eq!(min_key, expected_key, 
                        "Expected key {} but got {}", expected_key, min_key);
                    assert_eq!(val, expected_val,
                        "Expected value {} but got {}", expected_val, val);
                }
            }
        } else {
            panic!("Priority queue is empty but expected more elements");
        }
    }

    assert!(pq.delete_min(&temp_stack, guard).is_none(), 
        "Queue should be empty after deletions");
    println!("Test passed!");
    std::io::stdout().flush().unwrap();
}

fn test_concurrent_producer_consumer() {
    let test_start = Instant::now();
    let ops_counter = Arc::new(AtomicU32::new(0));
    let deadlock_detected = Arc::new(AtomicBool::new(false));
    let timeout_secs = 10; 

    println!("\n=== Testing concurrent producer-consumer (Timeout: {}s) ===", timeout_secs);
    std::io::stdout().flush().unwrap();

    let pq = Arc::new(MDList::new(DIMENSION, RANGE));

    let deadlock_flag = deadlock_detected.clone();
    thread::spawn({
        move || {
            thread::sleep(Duration::from_secs(timeout_secs));
            deadlock_flag.store(true, Ordering::SeqCst);
        }
    });

    thread::scope(|s| {
        s.spawn({
            let pq = pq.clone();
            let ops_counter = ops_counter.clone();
            let deadlock_detected = deadlock_detected.clone();
            move || {
                let guard = &epoch::pin();
                for i in 0..CONCURRENT_OPS {
                    if deadlock_detected.load(Ordering::SeqCst) {
                        println!("[PRODUCER] Deadlock detected - aborting");
                        return;
                    }
                    
                    pq.insert(i as u32, Box::into_raw(Box::new(i as u8)) as *mut u8);
                    ops_counter.fetch_add(1, Ordering::Relaxed);
                    
                    if i % 100 == 0 {
                        println!("[PRODUCER] Inserted {} (Total: {}) - Elapsed: {:.2}s", 
                            i, ops_counter.load(Ordering::Relaxed),
                            test_start.elapsed().as_secs_f32());
                        std::io::stdout().flush().unwrap();
                    }
                }
            }
        });

        s.spawn({
            let pq = pq.clone();
            let ops_counter = ops_counter.clone();
            let deadlock_detected = deadlock_detected.clone();
            move || {
                let guard = &epoch::pin();
                let mut count = 0;
                let head = pq.head_ptr(guard);
                let temp_stack = Stack {
                    head: Atomic::from(head),
                    del: std::array::from_fn(|_| Atomic::from(head)),
                };

                while count < CONCURRENT_OPS {
                    if deadlock_detected.load(Ordering::SeqCst) {
                        println!("[CONSUMER] Deadlock detected - aborting");
                        return;
                    }

                    if let Some(min_node) = pq.delete_min(&temp_stack, guard) {
                        unsafe {
                            let val_ptr = (*min_node.as_raw()).val.load(Ordering::Acquire);
                            if !val_ptr.is_null() {
                                count += 1;
                                ops_counter.fetch_add(1, Ordering::Relaxed);
                                
                                if count % 100 == 0 {
                                    println!("[CONSUMER] Deleted {} (Total: {}) - Elapsed: {:.2}s",
                                        count, ops_counter.load(Ordering::Relaxed),
                                        test_start.elapsed().as_secs_f32());
                                    std::io::stdout().flush().unwrap();
                                }
                            }
                        }
                    }
                }
                println!("[CONSUMER] Completed: deleted {} items", count);
                std::io::stdout().flush().unwrap();
            }
        });

        s.spawn({
            let ops_counter = ops_counter.clone();
            let deadlock_detected = deadlock_detected.clone();
            move || {
                let mut last_report = Instant::now();
                loop {
                    if deadlock_detected.load(Ordering::SeqCst) {
                        println!("\n!!! DEADLOCK DETECTED AFTER {}s !!!", timeout_secs);
                        println!("Final operation count: {}", ops_counter.load(Ordering::Relaxed));
                        std::process::exit(1);
                    }

                    if last_report.elapsed().as_secs() >= 1 {
                        println!("[MONITOR] Ops/sec: {:.1} - Total: {} - Elapsed: {:.1}s",
                            ops_counter.load(Ordering::Relaxed) as f32 / test_start.elapsed().as_secs_f32(),
                            ops_counter.load(Ordering::Relaxed),
                            test_start.elapsed().as_secs_f32());
                        last_report = Instant::now();
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        });
    });

    if !deadlock_detected.load(Ordering::SeqCst) {
        println!("Test completed in {:.2} seconds", test_start.elapsed().as_secs_f32());
        println!("Total operations: {}", ops_counter.load(Ordering::Relaxed));
        println!("Concurrent test passed!");
    }
    std::io::stdout().flush().unwrap();
}

fn test_concurrent_mixed_ops() {
    let test_start = Instant::now();
    let ops_counter = Arc::new(AtomicU32::new(0));
    let deadlock_detected = Arc::new(AtomicBool::new(false));
    let timeout_secs = 10;

    println!("\n=== Testing concurrent mixed operations (Timeout: {}s) ===", timeout_secs);
    std::io::stdout().flush().unwrap();

    let pq = Arc::new(MDList::new(DIMENSION, RANGE));

    let deadlock_flag = deadlock_detected.clone();
    thread::spawn({
        move || {
            thread::sleep(Duration::from_secs(timeout_secs));
            deadlock_flag.store(true, Ordering::SeqCst);
        }
    });

    thread::scope(|s| {
        for thread_id in 0..4 {
            s.spawn({
                let pq = pq.clone();
                let ops_counter = ops_counter.clone();
                let deadlock_detected = deadlock_detected.clone();
                move || {
                    let guard = &epoch::pin();
                    let head = pq.head_ptr(guard);
                    let temp_stack = Stack {
                        head: Atomic::from(head),
                        del: std::array::from_fn(|_| Atomic::from(head)),
                    };

                    for i in 0..CONCURRENT_OPS/4 {
                        if deadlock_detected.load(Ordering::SeqCst) {
                            println!("[THREAD {}] Deadlock detected - aborting", thread_id);
                            return;
                        }

                        let key = (thread_id * 1000) + i as u32;
                        pq.insert(key, Box::into_raw(Box::new(i as u8)) as *mut u8);
                        ops_counter.fetch_add(1, Ordering::Relaxed);

                        if i % 5 == 0 {
                            if let Some(min_node) = pq.delete_min(&temp_stack, guard) {
                                unsafe {
                                    let val_ptr = (*min_node.as_raw()).val.load(Ordering::Acquire);
                                    if !val_ptr.is_null() {
                                        ops_counter.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }

                        if i % 50 == 0 {
                            println!("[THREAD {}] Progress: {}/{} - Total ops: {}",
                                thread_id, i, CONCURRENT_OPS/4,
                                ops_counter.load(Ordering::Relaxed));
                        }
                    }
                    println!("[THREAD {}] Completed", thread_id);
                }
            });
        }

        s.spawn({
            let ops_counter = ops_counter.clone();
            let deadlock_detected = deadlock_detected.clone();
            move || {
                let mut last_report = Instant::now();
                loop {
                    if deadlock_detected.load(Ordering::SeqCst) {
                        println!("\n!!! DEADLOCK DETECTED AFTER {}s !!!", timeout_secs);
                        println!("Final operation count: {}", ops_counter.load(Ordering::Relaxed));
                        std::process::exit(1);
                    }

                    if last_report.elapsed().as_secs() >= 1 {
                        println!("[MONITOR] Ops/sec: {:.1} - Total: {} - Elapsed: {:.1}s",
                            ops_counter.load(Ordering::Relaxed) as f32 / test_start.elapsed().as_secs_f32(),
                            ops_counter.load(Ordering::Relaxed),
                            test_start.elapsed().as_secs_f32());
                        last_report = Instant::now();
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        });
    });

    if !deadlock_detected.load(Ordering::SeqCst) {
        println!("Test completed in {:.2} seconds", test_start.elapsed().as_secs_f32());
        println!("Total operations: {}", ops_counter.load(Ordering::Relaxed));
        println!("Mixed operations test passed!");
    }
    std::io::stdout().flush().unwrap();
}

fn main() {

    // Single-threaded tests
    test_scenario(
        "Random order insertion",
        vec![1000, 800, 1500], 
        vec![42, 33, 55]
    );

    test_scenario(
        "Ascending order insertion",
        vec![100, 200, 250, 255],
        vec![10, 20, 25, 26]
    );

    test_scenario(
        "Descending order insertion",
        vec![255, 200, 100, 50],
        vec![26, 20, 10, 5]
    );

    // Temporarily commented out - causing issues

    test_scenario(
        "Sparse keys with large gaps",
        vec![10, 1000, 5000, 100],
        vec![1, 100, 200, 10]
    );

    test_scenario(
        "Single element",
        vec![42],
        vec![99]
    );

    // Concurrent tests
    test_concurrent_producer_consumer();
    test_concurrent_mixed_ops();

    println!("\nAll tests completed successfully!");
    std::io::stdout().flush().unwrap();
}
// ... all your existing code ...

// Add this at the VERY END of the file:
#[cfg(test)]  // <- This means "only compile for tests"
mod tests {
    use super::*;  // <- Gives access to your existing code
    
    #[test]
    fn basic_test() {
        let pq = MDList::new(4, 10000); // Use your dimensions
        let guard = crossbeam::epoch::pin();
        
        // Insert one item
        pq.insert(1, Box::into_raw(Box::new(1)) as *mut u8);
        
        // Try to remove it
        let head = pq.head_ptr(&guard);
        let stack = Stack {
            head: Atomic::from(head),
            del: std::array::from_fn(|_| Atomic::from(head)),
        };
        assert!(pq.delete_min(&stack, &guard).is_some());
    }
}

#[test]
fn two_thread_test() {
    use std::sync::Arc;
    use std::thread;

    let pq = Arc::new(MDList::new(4, 10000));
    let guard = crossbeam::epoch::pin();

    let pq_clone = pq.clone();
    let inserter = thread::spawn(move || {
        pq_clone.insert(1, Box::into_raw(Box::new(1)) as *mut u8);
    });

    let head = pq.head_ptr(&guard);
    let stack = Stack {
        head: Atomic::from(head),
        del: std::array::from_fn(|_| Atomic::from(head)),
    };
    let _ = pq.delete_min(&stack, &guard);

    inserter.join().unwrap();
}

#[test]
fn three_thread_test() {
    use std::sync::Arc;
    use std::thread;

    let pq = Arc::new(MDList::new(4, 10000));
    let guard = crossbeam::epoch::pin();

    // Thread 1: Inserter
    let pq1 = pq.clone();
    let t1 = thread::spawn(move || {
        pq1.insert(1, Box::into_raw(Box::new(1)) as *mut u8);
    });

    // Thread 2: Inserter  
    let pq2 = pq.clone();
    let t2 = thread::spawn(move || {
        pq2.insert(2, Box::into_raw(Box::new(2)) as *mut u8);
    });

    // Thread 3: Remover
    let head = pq.head_ptr(&guard);
    let stack = Stack {
        head: Atomic::from(head),
        del: std::array::from_fn(|_| Atomic::from(head)),
    };
    let _ = pq.delete_min(&stack, &guard);

    t1.join().unwrap();
    t2.join().unwrap();
}

#[test]
fn four_thread_test() {
    use std::sync::Arc;
    use std::thread;
    use crossbeam::epoch;

    let pq = Arc::new(MDList::new(4, 10000));
    
    // Two inserters
    let pq1 = pq.clone();
    let t1 = thread::spawn(move || {
        pq1.insert(1, Box::into_raw(Box::new(1)) as *mut u8);
    });
    
    let pq2 = pq.clone();
    let t2 = thread::spawn(move || {
        pq2.insert(2, Box::into_raw(Box::new(2)) as *mut u8);
    });

    // Two deleters
    let pq3 = pq.clone();
    let t3 = thread::spawn(move || {
        let guard = epoch::pin();
        let head = pq3.head_ptr(&guard);
        let stack = Stack {
            head: Atomic::from(head),
            del: std::array::from_fn(|_| Atomic::from(head)),
        };
        pq3.delete_min(&stack, &guard);
    });

    let pq4 = pq.clone();
    let t4 = thread::spawn(move || {
        let guard = epoch::pin();
        let head = pq4.head_ptr(&guard);
        let stack = Stack {
            head: Atomic::from(head),
            del: std::array::from_fn(|_| Atomic::from(head)),
        };
        pq4.delete_min(&stack, &guard);
    });

    t1.join().unwrap();
    t2.join().unwrap(); 
    t3.join().unwrap();
    t4.join().unwrap();
}

#[test]
fn six_thread_test() {
    use std::sync::Arc;
    use std::thread;
    use crossbeam::epoch;

    let pq = Arc::new(MDList::new(4, 10000));
    
    // Three inserters
    let insert_values = vec![1, 2, 3];
    let insert_threads: Vec<_> = insert_values.into_iter().map(|val| {
        let pq = pq.clone();
        thread::spawn(move || {
            pq.insert(val, Box::into_raw(Box::new(val)) as *mut u8);
        })
    }).collect();

    // Three deleters
    let delete_threads: Vec<_> = (0..3).map(|_| {
        let pq = pq.clone();
        thread::spawn(move || {
            let guard = epoch::pin();
            let head = pq.head_ptr(&guard);
            let stack = Stack {
                head: Atomic::from(head),
                del: std::array::from_fn(|_| Atomic::from(head)),
            };
            pq.delete_min(&stack, &guard)
        })
    }).collect();

    // Join all
    for t in insert_threads.into_iter().chain(delete_threads) {
        t.join().unwrap();
    }
}