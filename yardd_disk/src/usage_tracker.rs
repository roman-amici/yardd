use std::{cmp::Ordering, time::SystemTime};

use priority_queue::PriorityQueue;

use crate::page::PageId;

#[derive(Eq, PartialEq, Ord)]
pub struct InverseSystemTime {
    time: SystemTime,
}

impl InverseSystemTime {
    pub fn now() -> Self {
        InverseSystemTime {
            time: SystemTime::now(),
        }
    }
}

impl PartialOrd for InverseSystemTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.time.partial_cmp(&other.time) {
            Some(ord) => match ord {
                Ordering::Less => Some(Ordering::Greater),
                Ordering::Equal => Some(Ordering::Equal),
                Ordering::Greater => Some(Ordering::Less),
            },
            None => None,
        }
    }
}

#[test]
fn test_inversion() {
    let t1 = InverseSystemTime {
        time: SystemTime::now(),
    };
    let t2 = InverseSystemTime {
        time: SystemTime::UNIX_EPOCH,
    };

    assert!(t1 < t2);
    assert!(t2 > t1);
}

#[test]
fn test_equality() {
    let time = SystemTime::now();

    let t1 = InverseSystemTime { time };
    let t2 = InverseSystemTime { time };

    assert!(t1 == t2);
}

pub struct UsageTracker {
    // Make this a trait
    pub last_used: PriorityQueue<PageId, InverseSystemTime>,
}

impl UsageTracker {
    pub fn new() -> Self {
        UsageTracker {
            last_used: PriorityQueue::new(),
        }
    }

    pub fn insert(&mut self, page_id: PageId) {
        self.last_used.push(page_id, InverseSystemTime::now());
    }

    pub fn touch(&mut self, page_id: PageId) {
        self.last_used
            .change_priority(&page_id, InverseSystemTime::now());
    }
}
