    use crossbeam::epoch::{self, Atomic, Owned, Shared, Guard};
    use std::sync::atomic::{AtomicU32, Ordering}; 
    use crossbeam::epoch::CompareExchangeError;
    use std::sync::atomic::{AtomicBool, AtomicPtr};
    use std::sync::Arc;
    use std::ops::Deref;

    static GLOBAL_SEQ: AtomicU32 = AtomicU32::new(1);

    pub trait AtomicMarking<T> {
        fn load_marked<'g>(&self, guard: &'g Guard) -> Shared<'g, T>;

        fn compare_and_set_marked<'g>(
            &self,
            current: Shared<'g, T>,
            new: Shared<'g, T>,
            guard: &'g Guard,
        ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, Shared<'g, T>>>;
    }

impl<T> AtomicMarking<T> for Atomic<T> {
    fn load_marked<'g>(&self, guard: &'g Guard) -> Shared<'g, T> {
        self.load(Ordering::Acquire, guard)
    }

    fn compare_and_set_marked<'g>(
        &self,
        current: Shared<'g, T>,
        new: Shared<'g, T>,
        guard: &'g Guard,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T, Shared<'g, T>>> {
        self.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire, guard)
    }
}


pub const DIMENSION: usize = 8;
const CACHE_LINE_SIZE: usize = 64;

const FADP: usize = 0b001;
const FPRG: usize = 0b010;
const FDEL: usize = 0b100;

const ADPINV_MASK: usize = 1;
const PRGINV_MASK: usize = 2;
const INVALID_MASK: usize = 3;
const MARKED_MASK: usize = 1;
const DELETED_MASK: usize = 1;

pub struct Desc {
    curr: Atomic<Node>,
    dp: u8,      
    dc: u8,          
}

pub struct Node {
    pub child: [Atomic<Node>; DIMENSION],
    pub key: u32,
    pub coord: [u32; DIMENSION],
    pub seq: u32,
    pub purged: Atomic<Node>,
    pub pending: Atomic<Desc>,
    pub val: AtomicPtr<u8>,
}


pub struct HeadNode {
    pub node: Arc<Node>,
    pub ver: AtomicU32,
}

pub struct PriorityQueue {
    pub d: usize,
    pub n: usize,
    pub r: usize,
    pub head: Arc<HeadNode>,
    pub stack: Arc<Stack>,
}

#[derive(Clone)]
pub struct Stack {
    pub head: Atomic<Node>,
    pub del: [Atomic<Node>; DIMENSION],
}

pub struct MDList {
    dimension: usize, 
    range: usize,    
    head: Atomic<Node>,
    _pad: [u8; CACHE_LINE_SIZE - std::mem::size_of::<Atomic<Node>>()],
    stack: Atomic<Stack>,
    _pad1: [u8; CACHE_LINE_SIZE - std::mem::size_of::<Atomic<Shared<Stack>>>()],
    purge: Atomic<Stack>,
    marked_node: AtomicU32,
    r: u32,
    purging: AtomicBool,
}

impl Node {
    pub fn new(key: u32, coord: [u32; DIMENSION], val: Option<*mut u8>, seq: u32) -> Self {
    Node {
        child: array_init::array_init(|_| Atomic::null()),
        key,
        coord,
        seq,
        purged: Atomic::null(),
        pending: Atomic::null(),
        val: AtomicPtr::new(val.unwrap_or(std::ptr::null_mut())),
    }
    }

    pub fn clone_without_children(&self) -> Self {
    Node {
        child: array_init::array_init(|_| Atomic::null()),
        key: self.key,
        coord: self.coord,
        seq: self.seq,
        purged: Atomic::null(),
        pending: Atomic::null(),
        val: AtomicPtr::new(self.val.load(std::sync::atomic::Ordering::Relaxed)),
    }
    }
    

    pub fn new_fdel(seq: u32) -> Self {
    Node {
        child: array_init::array_init(|_| Atomic::null()),
        key: 0,
        coord: [0; DIMENSION],
        seq,
        purged: Atomic::null(),
        pending: Atomic::null(),
        val: AtomicPtr::new(std::ptr::null_mut()),
    }
    }
}

impl MDList {
    pub fn head_ptr<'g>(&self, guard: &'g crossbeam::epoch::Guard) -> crossbeam::epoch::Shared<'g, Node> {
        self.head.load(std::sync::atomic::Ordering::Acquire, guard)
    }
}


impl MDList {
    pub fn new(dimension: usize, range: usize) -> Self {
        let coord = [0u32; DIMENSION];
        let guard = &crossbeam::epoch::pin();
    
        let head_node = Node::new(0, coord.clone(), None, dimension as u32);
        let head_shared = Owned::new(head_node).into_shared(guard);
        let head_atomic = Atomic::from(head_shared);
    
        let owned_stack = Owned::new(Stack {
            head: Atomic::from(head_shared),
            del: std::array::from_fn(|_| Atomic::from(head_shared)),
        });
        let shared_stack = owned_stack.into_shared(guard);
    
    let mdlist = MDList {
        head: head_atomic,
        _pad: [0u8; CACHE_LINE_SIZE - std::mem::size_of::<Atomic<Node>>()],
        stack: Atomic::null(),
        _pad1: [0u8; CACHE_LINE_SIZE - std::mem::size_of::<Atomic<Shared<Stack>>>()],
        purge: Atomic::null(),
        marked_node: AtomicU32::new(0),
        r: 0,
        purging: AtomicBool::new(false),
        dimension,
        range,
    };
    
        mdlist.stack.store(shared_stack, Ordering::Release);
    
        mdlist
    }    
}


pub fn set_adpinv<'g, T>(ptr: Shared<'g, T>) -> Shared<'g, T> {
    ptr.with_tag(ptr.tag() | ADPINV_MASK)
}

pub fn clr_adpinv<'g, T>(ptr: Shared<'g, T>) -> Shared<'g, T> {
    ptr.with_tag(ptr.tag() & !ADPINV_MASK)
}

pub fn is_adpinv<'g, T>(ptr: Shared<'g, T>) -> bool {
    ptr.tag() & ADPINV_MASK != 0
}


pub fn set_prginv<'g>(ptr: Shared<'g, Node>) -> Shared<'g, Node> {
    ptr.with_tag(ptr.tag() | PRGINV_MASK)
}

pub fn clr_prginv<'g>(ptr: Shared<'g, Node>) -> Shared<'g, Node> {
    ptr.with_tag(ptr.tag() & !PRGINV_MASK)
}

pub fn is_prginv<'g>(ptr: Shared<'g, Node>) -> bool {
    ptr.tag() & PRGINV_MASK != 0
}

pub fn clr_invalid<'g>(ptr: Shared<'g, Node>) -> Shared<'g, Node> {
    ptr.with_tag(ptr.tag() & !INVALID_MASK)
}

pub fn is_invalid<'g>(ptr: Shared<'g, Node>) -> bool {
    ptr.tag() & INVALID_MASK != 0
}

pub fn set_marked<'g>(ptr: Shared<'g, Stack>) -> Shared<'g, Stack> {
    ptr.with_tag(ptr.tag() | MARKED_MASK)
}

pub fn clr_marked<'g>(ptr: Shared<'g, Stack>) -> Shared<'g, Stack> {
    ptr.with_tag(ptr.tag() & !MARKED_MASK)
}

pub fn set_deleted<'g>(ptr: Shared<'g, Node>) -> Shared<'g, Node> {
    ptr.with_tag(ptr.tag() | DELETED_MASK)
}

pub fn clr_deleted<'g>(ptr: Shared<'g, Node>) -> Shared<'g, Node> {
    ptr.with_tag(ptr.tag() & !DELETED_MASK)
}

pub fn is_deleted<'g>(ptr: Shared<'g, Node>) -> bool {
    ptr.tag() & DELETED_MASK != 0
}

pub fn set_mark_ptr<'g, T>(ptr: Shared<'g, T>, mark: usize) -> Shared<'g, T> {
    ptr.with_tag(ptr.tag() | mark)
}

pub fn clear_mark<'g, T>(ptr: Shared<'g, T>, mark: usize) -> Shared<'g, T> {
    ptr.with_tag(ptr.tag() & !mark)
}

pub fn is_marked<'g, T>(ptr: Shared<'g, T>, mark: usize) -> bool {
    ptr.tag() & mark != 0
}


pub fn key_to_coord(key: u32) -> [u32; DIMENSION] {
    const BASIS: u32 = 16; 
    let mut coord = [0u32; DIMENSION];
    let mut quotient = key;

    for i in (0..DIMENSION).rev() {
        coord[i] = quotient % BASIS;
        quotient /= BASIS;
    }

    coord
}

impl MDList {
    fn next_seq() -> u32 {
        GLOBAL_SEQ.fetch_add(1, Ordering::Relaxed)
    }

pub fn insert(&self, key: u32, val: *mut u8) {
    let guard = &epoch::pin();
    let coord = key_to_coord(key);
    let new_node = Owned::new(Node::new(key, coord, Some(val), Self::next_seq()));
    let new_ptr = new_node.into_shared(guard);
    
    loop {
        let mut pred = Shared::null();
        let mut curr = Shared::null();
        let mut dp = 0;
        let mut dc = 0;
        let mut stack = Stack {
            head: Atomic::null(),
            del: std::array::from_fn(|_| Atomic::null()),
        };

        self.locate_pred(&coord, &mut pred, &mut curr, &mut dp, &mut dc, &mut stack, guard);

    unsafe {
        let mut prev = self.head.load(Ordering::Acquire, guard);
        let mut next = prev.deref().child[0].load(Ordering::Acquire, guard);
        
        while !next.is_null() && next.deref().key < key {
            prev = next;
            next = next.deref().child[0].load(Ordering::Acquire, guard);
        }
        
        new_ptr.deref().child[0].store(next, Ordering::Release);
        prev.deref().child[0].store(new_ptr, Ordering::Release);
    }

    for d in 1..DIMENSION {
        unsafe {
            new_ptr.deref().child[d].store(
                stack.del[d].load(Ordering::Acquire, guard),
                Ordering::Release
            );

            if d == dp && !pred.is_null() {
                pred.deref().child[d].store(new_ptr, Ordering::Release);
            } else {
                self.head.load(Ordering::Acquire, guard)
                    .deref()
                    .child[d]
                    .store(new_ptr, Ordering::Release);
            }
        }
    }

    self.rewind_stack(key, dc, pred, &stack, guard);
    break;
    }
    }
    
}

    
impl MDList {
    pub fn locate_pred<'g>(
        &self,
        coord: &[u32; DIMENSION],
        pred: &mut Shared<'g, Node>,
        curr: &mut Shared<'g, Node>,
        dp: &mut usize,
        dc: &mut usize,
        stack: &Stack,
        guard: &'g Guard,
    ) {
        *curr = self.head.load(Ordering::Acquire, guard);
        *dc = 0;
        *dp = 0;
    
        while *dc < DIMENSION {
            if *dc >= DIMENSION {
                break;
            }
    
            while !curr.is_null() {
                let curr_node = unsafe { &*curr.as_raw() };
    
                if curr_node.coord[*dc] < coord[*dc] {
                    *pred = *curr;
                    *dp = *dc;
                    self.finish_inserting(*curr, *dc, *dc, guard);
                    *curr = curr_node.child[*dc].load(Ordering::Acquire, guard);
                } else {
                    break;
                }
            }
    
            if curr.is_null() {
                break;
            }
    
            let curr_node = unsafe { &*curr.as_raw() };
    
            if curr_node.coord[*dc] > coord[*dc] {
                *dc = DIMENSION;
                break;
            } 
            else if curr_node.coord[*dc] == coord[*dc] {
                if *dc < DIMENSION {
                    unsafe {
                        stack.del[*dc].store(*curr, Ordering::Relaxed);
                    }
                }
                *dc += 1;
            }
            else {
                *pred = *curr;
                *dp = *dc;
                self.finish_inserting(*curr, *dc, *dc, guard);
                *curr = curr_node.child[*dc].load(Ordering::Acquire, guard);
            }
        }
    
        *dp = (*dp).min(DIMENSION - 1);
    }
}


impl MDList {
    pub fn fill_new_node<'g>(
        &self,
        node_ptr: Shared<'g, Node>,
        curr: Shared<'g, Node>,
        dp: usize,
        dc: usize,
        guard: &'g Guard,
    ) {
        let node = unsafe { node_ptr.deref() };
        node.pending.store(Shared::null(), Ordering::Relaxed);

        if dp < dc {
            let desc = Desc {
                curr: Atomic::from(curr),
                dp: dp as u8,
                dc: dc as u8,
            };
            node.pending.store(Owned::new(desc).into_shared(guard), Ordering::Release);
        }

        for i in 0..dp {
            node.child[i].store(Shared::null(), Ordering::Relaxed);
        }

        for i in dp..DIMENSION {
            node.child[i].store(Shared::null(), Ordering::Relaxed);
        }

        if dc < DIMENSION {
            node.child[dc].store(curr, Ordering::Relaxed);
        }
    }
}


impl MDList {
    pub fn finish_inserting<'g>(
        &self,
        node: Shared<'g, Node>,
        dp: usize,
        dc: usize,
        guard: &'g Guard,
    ) {
        if node.is_null() {
            return;
        }

        let n = unsafe { node.deref() };
        let desc_ptr = n.pending.load(Ordering::Acquire, guard);
        if desc_ptr.is_null() {
            return;
        }

        let desc = unsafe { desc_ptr.deref() };

        if dc < desc.dp as usize || dp > desc.dc as usize {
            return;
        }

        let curr_ptr = desc.curr.load(Ordering::Acquire, guard);
        if curr_ptr.is_null() {
            return;
        }
        let curr = unsafe { curr_ptr.deref() };

        for i in desc.dp as usize..desc.dc as usize {
            let mut child = curr.child[i].load(Ordering::Acquire, guard);

        while !is_adpinv(child) && !is_prginv(child) {
            let new_ptr = set_adpinv(child);
            let result = curr.child[i].compare_exchange(
                child,
                new_ptr,
                Ordering::AcqRel,
                Ordering::Acquire,
                guard,
            );
            match result {
                Ok(_) => break,
                Err(e) => {
                    child = e.current;
                }
            }
        }

        let clean_child = clr_adpinv(child);
        let _ = n.child[i].compare_exchange(
            Shared::null(),
            clean_child,
            Ordering::AcqRel,
            Ordering::Acquire,
            guard,
        );
    }

    let _ = n.pending.compare_exchange(
        desc_ptr,
        Shared::null(),
        Ordering::AcqRel,
        Ordering::Acquire,
        guard,
    );
    }
}




impl MDList {
    pub fn rewind_stack<'g>(
        &self,
        key: u32,
        dp: usize,
        pred: Shared<'g, Node>,
        stack: &Stack,
        guard: &'g Guard,
    ) {
    let mut old_shared = self.stack.load(Ordering::Acquire, guard);
    let dp = dp.min(DIMENSION - 1); 

    loop {
        let old = unsafe { old_shared.deref() };

        let old_head_ptr = old.head.load(Ordering::Acquire, guard);
        if old_head_ptr.is_null() {
            break;
        }

        let old_head_node = unsafe { old_head_ptr.deref() };
        let old_head_seq = old_head_node.seq;

        let new_stack = Stack {
            head: Atomic::from(old_head_ptr),
            del: std::array::from_fn(|i| old.del[i].clone()),
        };

        let last_del = old.del[DIMENSION - 1].load(Ordering::Acquire, guard);
        if last_del.is_null() {
            break;
        }
        let last_key = unsafe { last_del.deref().key };

        let current_head_ptr = new_stack.head.load(Ordering::Acquire, guard);
        if current_head_ptr.is_null() {
            break;
        }
        let current_head_seq = unsafe { current_head_ptr.deref().seq };

        if current_head_seq == old_head_seq {
            if key <= last_key {
                for i in dp..DIMENSION {
                    if i >= DIMENSION { break; }  
                    stack.del[i].store(pred, Ordering::Relaxed);
                }
            } else {
                break;
            }
        } else if current_head_seq > old_head_seq {
            let prg_ptr = clear_mark(old_head_node.purged.load(Ordering::Acquire, guard), FPRG);
            if !prg_ptr.is_null() {
                let prg_node = unsafe { prg_ptr.deref() };
                if prg_node.key <= last_key {
                    let new_head = clear_mark(prg_node.purged.load(Ordering::Acquire, guard), FPRG);
                    if !new_head.is_null() {
                        new_stack.head.store(new_head, Ordering::Relaxed);
                        for i in 0..DIMENSION {
                            stack.del[i].store(new_head, Ordering::Relaxed);
                        }
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        } else {
            let prg_ptr = clear_mark(old_head_node.purged.load(Ordering::Acquire, guard), FPRG);
            if !prg_ptr.is_null() {
                let prg_node = unsafe { prg_ptr.deref() };
                if prg_node.key <= key {
                    let new_head = clear_mark(prg_node.purged.load(Ordering::Acquire, guard), FPRG);
                    if !new_head.is_null() {
                        new_stack.head.store(new_head, Ordering::Relaxed);
                        for i in 0..DIMENSION {
                            stack.del[i].store(new_head, Ordering::Relaxed);
                        }
                    }
                } else {
                    for i in dp..DIMENSION {
                        if i >= DIMENSION { break; }
                        stack.del[i].store(pred, Ordering::Relaxed);
                    }
                }
            } else {
                break;
            }
        }
    
        let new_shared = Owned::new(new_stack).into_shared(guard);
        match self.stack.compare_exchange(
            old_shared,
            new_shared,
            Ordering::AcqRel,
            Ordering::Acquire,
            guard,
        ) {
            Ok(_) => break,
            Err(e) => old_shared = e.current,
        }
        }
    }
}
    
    
impl MDList {
    pub fn delete_min<'g>(&self, stack: &Stack, guard: &'g Guard) -> Option<Shared<'g, Node>> {
        let head = self.head.load(Ordering::Acquire, guard);
        let head_node = unsafe { head.deref() };
        

    let mut curr = head_node.child[0].load(Ordering::Acquire, guard);
        
    while !curr.is_null() {
        let node = unsafe { curr.deref() };
        let val_ptr = node.val.load(Ordering::Acquire);
        
        if !val_ptr.is_null() {
            let val_shared = unsafe { Shared::from(val_ptr as *const u8) };
            
            if !is_marked(val_shared, FDEL) {
                let mark_ptr = set_mark_ptr(val_shared, FDEL);
                if node.val.compare_exchange(
                    val_ptr,
                    mark_ptr.as_raw() as *mut u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ).is_ok() {
                    let next = node.child[0].load(Ordering::Acquire, guard);
                    head_node.child[0].store(next, Ordering::Release);
                    return Some(curr);
                }
            }
        }
        
        curr = node.child[0].load(Ordering::Acquire, guard);
    }
        
        None
    }
}


impl MDList {
    pub fn purge<'g>(
        &self,
        hd: Shared<'g, Node>,
        prg: Shared<'g, Node>,
        guard: &'g Guard,
    ) {
        let current_head = self.head.load(Ordering::Acquire, guard);
        if hd.as_raw() != current_head.as_raw() {
            return;
        }
    
        let prg_ref = unsafe { prg.deref() };
        let prgcopy = Owned::new(Node::clone_without_children(prg_ref));
        let hdnew = Owned::new(unsafe { Node::new_fdel(current_head.deref().seq + 1) });
    
        let mut d = 0;
        let mut pnt = hd;
    
        while d < DIMENSION {
            if !self.locate_pivot(&mut pnt, prg, d, guard) {
                pnt = hd;
                d = 0;
                continue;
            }
    
            let child = unsafe { pnt.deref() }.child[d].load(Ordering::Acquire, guard);
    
            if pnt.as_raw() == hd.as_raw() {
                hdnew.child[d].store(child, Ordering::Release);
                let marked_null = set_mark_ptr(Shared::null(), FDEL);
                unsafe {
                    prgcopy.deref().child[d].store(marked_null, Ordering::Release);
                }
            } else {
                unsafe {
                    prgcopy.deref().child[d].store(child, Ordering::Release);
                }
            }
    
            d += 1;
        }
    
        let shared_prgcopy = prgcopy.into_shared(guard);
    
        let last_child = unsafe { shared_prgcopy.deref().child[DIMENSION - 1].load(Ordering::Acquire, guard) };
        if is_marked(last_child, FDEL) {
            hdnew.child[DIMENSION - 1].store(shared_prgcopy, Ordering::Release);
        }
    
        let shared_hdnew = hdnew.into_shared(guard); 
    
        let desc1 = Box::new(Desc {
            curr: Atomic::from(shared_hdnew),
            dp: 0,
            dc: 0,
        });
        let desc_ptr1: *mut Desc = Box::into_raw(desc1);
        let desc_shared1 = unsafe { Shared::from(desc_ptr1 as *const Desc) };
        let marked_desc1 = set_mark_ptr(desc_shared1, FDEL);
        unsafe { prg.deref() }.pending.store(marked_desc1, Ordering::Release);
    
        let desc2 = Box::new(Desc {
            curr: Atomic::from(prg),
            dp: 0,
            dc: 0,
        });
        let desc_ptr2: *mut Desc = Box::into_raw(desc2);
        let desc_shared2 = unsafe { Shared::from(desc_ptr2 as *const Desc) };
        let marked_desc2 = set_mark_ptr(desc_shared2, FDEL);
        unsafe { hd.deref() }.pending.store(marked_desc2, Ordering::Release);
    
        self.head.store(shared_hdnew, Ordering::Release);
    }
    
    
    
    
pub fn locate_pivot<'g>(
    &self,
    pnt: &mut Shared<'g, Node>,
    prg: Shared<'g, Node>,
    d: usize,
    guard: &'g Guard,
) -> bool {
    while {
        let prg_ref: &Node = unsafe { prg.deref() };       
        let pnt_ref: &Node = unsafe { pnt.as_ref().unwrap() };       
        prg_ref.coord[d] > pnt_ref.coord[d]
    } {
        self.finish_inserting(*pnt, d, d, guard);

        let pnt_node: &Node = unsafe { pnt.as_ref().unwrap() };       
        let child = pnt_node.child[d].load(Ordering::Acquire, guard);
        *pnt = clear_mark(child, FADP | FPRG);
    }

    loop {
        let pnt_node: &Node = unsafe { pnt.as_ref().unwrap() };

        let child = pnt_node.child[d].load(Ordering::Acquire, guard);
        let marked = set_mark_ptr(child, FPRG);

        if pnt_node.child[d]
            .compare_exchange(child, marked, Ordering::AcqRel, Ordering::Acquire, guard)
            .is_ok()
        {
            break;
        } else if is_marked(child, FADP | FPRG) {
            break;
        }
    }

    let pnt_node: &Node = unsafe { pnt.as_ref().unwrap() };
    let child = pnt_node.child[d].load(Ordering::Acquire, guard);
    if is_marked(child, FPRG) {
        *pnt = clear_mark(child, FPRG);
        return true;
    }

    false
}


}