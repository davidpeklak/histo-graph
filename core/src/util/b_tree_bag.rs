//! This module defines a `BTreeBag` collection of elements that implement `Ord`.
//!
//! The implementation has the following goals:
//! 1) Insert and remove elements in a time that is similar to O(log(n)).
//! 2) Allow for duplicate values.
//! 3) Provide an iterator that visits the elements in sorted order.
//! 4) Implement Hash, PartialOrd.

use std::collections::btree_map::BTreeMap;

/// A `BTreeBag` is a collection that can hold the same element more than once, and provides "excellent performance"
/// for insertion and removal.
///
/// # Examples
///
/// ```
/// use histo_graph_core::util::b_tree_bag::BTreeBag;
///
/// let mut bag: BTreeBag<u32> = BTreeBag::new();
/// bag.insert(1);
/// bag.insert(2);
/// bag.insert(2);
///
/// for &i in bag.iter() {
///     println!("{}", i);
/// }
/// ```
#[derive(Debug, Hash, PartialEq)]
pub struct BTreeBag<T>
    where T: Ord {
    inner: BTreeMap<T, usize>
}

struct DuplicationIter<'a, T> {
    element: &'a T,
    total_count: usize,
    current_count: usize,
}

impl<'a, T> From<(&'a T, &'a usize)> for DuplicationIter<'a, T> {
    fn from(kv: (&'a T, &'a usize)) -> DuplicationIter<'a, T> {
        DuplicationIter {
            element: kv.0,
            total_count: *kv.1,
            current_count: 0,
        }
    }
}

impl<'a, T> Iterator for DuplicationIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        if self.current_count < self.total_count {
            self.current_count += 1;
            Some(self.element)
        } else {
            None
        }
    }
}

impl<T> BTreeBag<T>
    where T: Ord {

    /// Creates an empty BTreeBag
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::util::b_tree_bag::BTreeBag;
    ///
    /// let mut bag: BTreeBag<u32> = BTreeBag::new();
    /// ```
    pub fn new() -> BTreeBag<T> {
        BTreeBag {
            inner: BTreeMap::new(),
        }
    }

    /// Inserts an element into the BTreeBag.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::util::b_tree_bag::BTreeBag;
    ///
    /// let mut bag: BTreeBag<u32> = BTreeBag::new();
    /// bag.insert(1);
    /// ```
    pub fn insert(&mut self, t: T) {
        self.inner
            .entry(t)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    /// Removes an element from the BTreeBag.
    /// Returns true if the BTreeBag contained the element before the removal.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::util::b_tree_bag::BTreeBag;
    ///
    /// let mut bag: BTreeBag<u32> = BTreeBag::new();
    /// bag.insert(1);
    /// assert!(bag.remove(&1));
    /// assert!(!bag.remove(&1));
    /// ```
    pub fn remove(&mut self, t: &T) -> bool {
        let mut has_been_removed = false;
        let must_remove = self.inner
            .get_mut(t)
            .map(|count| {
                *count -= 1;
                has_been_removed = true;
                *count == 0
            });

        match must_remove {
            Some(true) => { self.inner.remove(t); }
            _ => ()
        }

        has_been_removed
    }

    /// An iterator that visits the elements in the BTreeBag in sorted order.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::util::b_tree_bag::BTreeBag;
    ///
    /// let mut bag: BTreeBag<u32> = BTreeBag::new();
    /// bag.insert(1);
    /// bag.insert(3);
    /// bag.insert(2);
    ///
    /// let vec: Vec<&u32> = bag.iter().collect();
    /// assert_eq!(vec, vec![&1, &2, &3]);
    /// ```
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=&'a T> {
        self.inner.iter().flat_map(|kv| {
            let i: DuplicationIter<'a, T> = kv.into();
            i
        })
    }

    /// Returns the number of elements in the BTreeBag.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::util::b_tree_bag::BTreeBag;
    ///
    /// let mut bag: BTreeBag<u32> = BTreeBag::new();
    /// bag.insert(1);
    /// bag.insert(2);
    /// bag.insert(2);
    ///
    /// assert_eq!(bag.len(), 3);
    /// ```
    pub fn len(&self) -> usize {
        self.iter().count()
    }
}

#[cfg(test)]
mod test {
    use super::BTreeBag;
    use crate::graph::graph::{Edge, VertexId};

    use rand::{thread_rng, Rng};

    #[test]
    fn test_insert_one() {
        let mut btb: BTreeBag<Edge> = BTreeBag::new();
        btb.insert(Edge(VertexId(0), VertexId(1)));

        let rslt: Vec<&Edge> = btb.iter().collect();
        assert_eq!(rslt, vec![&Edge(VertexId(0), VertexId(1))]);
    }

    #[test]
    fn test_insert_two_different() {
        let mut btb: BTreeBag<Edge> = BTreeBag::new();
        btb.insert(Edge(VertexId(0), VertexId(1)));
        btb.insert(Edge(VertexId(1), VertexId(2)));

        let rslt: Vec<&Edge> = btb.iter().collect();
        assert_eq!(rslt, vec![&Edge(VertexId(0), VertexId(1)), &Edge(VertexId(1), VertexId(2))]);
    }

    #[test]
    fn test_insert_two_different_in_opposite_order() {
        let mut btb: BTreeBag<Edge> = BTreeBag::new();
        btb.insert(Edge(VertexId(1), VertexId(2)));
        btb.insert(Edge(VertexId(0), VertexId(1)));

        let rslt: Vec<&Edge> = btb.iter().collect();
        assert_eq!(rslt, vec![&Edge(VertexId(0), VertexId(1)), &Edge(VertexId(1), VertexId(2))]);
    }

    #[test]
    fn test_insert_two_equal() {
        let mut btb: BTreeBag<Edge> = BTreeBag::new();
        btb.insert(Edge(VertexId(0), VertexId(1)));
        btb.insert(Edge(VertexId(0), VertexId(1)));

        let rslt: Vec<&Edge> = btb.iter().collect();
        assert_eq!(rslt, vec![&Edge(VertexId(0), VertexId(1)), &Edge(VertexId(0), VertexId(1))]);
    }

    #[test]
    fn test_insert_many() {
        let mut rng = thread_rng();

        // create a vector with 30 edges, with VertexIds between 0 and 3. That creates many
        // duplicate edges.
        let mut edges: Vec<Edge> = vec![];
        for _ in 0..30 {
            edges.push(Edge(VertexId(rng.gen_range(0u64, 3u64)), VertexId(rng.gen_range(0u64, 3u64))))
        }

        let mut btb: BTreeBag<Edge> = BTreeBag::new();

        // insert the edges into the BTreeBag
        for e in edges.iter() {
            btb.insert(*e);
        }

        // the iterator of the BTreeMap should visit the edges in sorted order
        let rslt: Vec<Edge> = btb.iter().map(|edge| *edge).collect();
        let sorted_edges = {edges.sort(); edges };
        assert_eq!(rslt,sorted_edges);
    }

    #[test]
    fn remove_one_of_one() {
        let mut btb: BTreeBag<Edge> = BTreeBag::new();
        btb.insert(Edge(VertexId(0), VertexId(1)));
        btb.remove(&Edge(VertexId(0), VertexId(1)));

        let rslt: Vec<&Edge> = btb.iter().collect();
        assert_eq!(rslt, Vec::<&Edge>::new());
    }

    #[test]
    fn remove_one_of_two() {
        let mut btb: BTreeBag<Edge> = BTreeBag::new();
        btb.insert(Edge(VertexId(0), VertexId(1)));
        btb.insert(Edge(VertexId(1), VertexId(2)));
        btb.remove(&Edge(VertexId(0), VertexId(1)));

        let rslt: Vec<&Edge> = btb.iter().collect();
        assert_eq!(rslt, vec![&Edge(VertexId(1), VertexId(2))]);
    }

    #[test]
    fn remove_one_of_two_equal() {
        let mut btb: BTreeBag<Edge> = BTreeBag::new();
        btb.insert(Edge(VertexId(0), VertexId(1)));
        btb.insert(Edge(VertexId(0), VertexId(1)));
        btb.remove(&Edge(VertexId(0), VertexId(1)));

        let rslt: Vec<&Edge> = btb.iter().collect();
        assert_eq!(rslt, vec![&Edge(VertexId(0), VertexId(1))]);
    }
}