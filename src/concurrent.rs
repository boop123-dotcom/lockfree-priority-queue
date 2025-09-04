// tests/concurrent.rs
use lockprio::mdlist::MDList;
use crossbeam::epoch;
use loom::sync::Arc;
use loom::thread;

#[test]
fn basic_concurrent() {
    loom::model(|| {
        let pq = Arc::new(MDList::new(4, 10000)); // Use your DIMENSION and RANGE
        
        let guard = &epoch::pin();
        let head = pq.head_ptr(guard);
        let stack = lockprio::mdlist::Stack {
            head: crossbeam::epoch::Atomic::from(head),
            del: std::array::from_fn(|_| crossbeam::epoch::Atomic::from(head)),
        };

        let pq1 = pq.clone();
        let t1 = thread::spawn(move || {
            pq1.insert(1, Box::into_raw(Box::new(1)) as *mut u8);
        });

        let pq2 = pq.clone();
        let t2 = thread::spawn(move || {
            pq2.delete_min(&stack, guard);
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}