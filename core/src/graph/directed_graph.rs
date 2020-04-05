use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use crate::graph::graph::*;
use crate::util::b_tree_bag::BTreeBag;
use std::collections::btree_map::BTreeMap;

/// A directed graph structure that doesn't contain any information concerning the vertex or the
/// edge attributes
#[derive(Debug, PartialEq)]
pub struct DirectedGraph {
    // Each edge is indexed for by of both its vertices => 1 edge appears twice in the map
    edge_map: HashMap<VertexId, BTreeBag<Edge>>,
}

impl DirectedGraph {

    /// Creates an empty `DirectedGraph`.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    ///
    /// let mut graph = DirectedGraph::new();
    /// ```
    pub fn new() -> DirectedGraph {
        DirectedGraph {
            edge_map: HashMap::new(),
        }
    }

    /// Returns true if the graph contains no vertices nor edges.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::VertexId;
    ///
    /// let mut g = DirectedGraph::new();
    /// assert!(g.is_empty());
    /// g.add_vertex(VertexId(1));
    /// assert!(!g.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.vertex_count() == 0
    }

    /// Returns the number of vertices in the graph
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::VertexId;
    ///
    /// let mut g = DirectedGraph::new();
    /// assert_eq!(g.vertex_count(), 0);
    /// g.add_vertex(VertexId(1));
    /// assert_eq!(g.vertex_count(), 1);
    /// ```
    pub fn vertex_count(&self) -> usize {
        self.edge_map.len()
    }

    /// Returns the number of edges in the graph
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// assert_eq!(g.edge_count(), 0);
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// assert_eq!(g.edge_count(), 1);
    /// ```
    pub fn edge_count(&self) -> usize {
        let mut total_count: usize = 0;
        for (_, edges) in &self.edge_map {
            let count: usize = edges
                .iter()
                .map(|&Edge(v1, v2)| if v1 == v2 { 2 } else { 1 })
                .sum();
            total_count += count;
        }
        // each edge is saved twice => the count should be a multiple of 2, and divided by 2
        total_count / 2
    }

    /// Returns true if the graph contains the `vertex_id`
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::VertexId;
    ///
    /// let mut g = DirectedGraph::new();
    /// assert!(!g.contains_vertex(VertexId(1)));
    /// g.add_vertex(VertexId(1));
    /// assert!(g.contains_vertex(VertexId(1)));
    /// ```
    pub fn contains_vertex(&self, vertex_id: VertexId) -> bool {
        self.edge_map.contains_key(&vertex_id)
    }

    /// An iterator visiting all vertices of the graph in arbitrary order.
    /// The iterator element type is `&VertexId`
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    ///
    /// for &v in g.vertices() {
    ///     println!("{:?}", v);
    /// }
    /// ```
    pub fn vertices(&self) -> impl Iterator<Item=&VertexId> {
        self.edge_map.keys()
    }

    /// Returns true if the graph contains the `edge`.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// assert!(!g.contains_edge(Edge(VertexId(1), VertexId(2))));
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// assert!(g.contains_edge(Edge(VertexId(1), VertexId(2))));
    /// ```
    pub fn contains_edge(&self, edge: Edge) -> bool {
        let Edge(v1, v2) = edge;
        if self.contains_vertex(v1) && self.contains_vertex(v2) {
            // We need to look-up only for one of the vertices
            self.edge_map
                .get(&v1)
                .unwrap()
                .iter()
                .position(|x| *x == edge)
                .is_some()
        } else {
            false
        }
    }

    /// An iterator visiting all the edges of the graph in arbitrary order.
    /// The iterator element type is `&Edge`
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// g.add_edge(Edge(VertexId(2), VertexId(3)));
    ///
    /// for &e in g.edges() {
    ///     println!("{:?}", e);
    /// }
    ///
    /// ```
    pub fn edges(&self) -> impl Iterator<Item = &Edge> {
        self.edge_map.iter()
            .map(|(vertex_id, bag)| {
                bag.iter()
                    .filter(move |Edge(from, _)| *vertex_id == *from)
            })
            .flatten()
    }

    /// An iterator visiting all the outbound edges of `vertex_id`.
    /// The iterator element type is `&Edge`
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// g.add_edge(Edge(VertexId(1), VertexId(3)));
    ///
    /// for &e in g.outbound_edges(VertexId(1)) {
    ///     println!("{:?}", e);
    /// }
    ///
    /// ```
    pub fn outbound_edges(&self, vertex_id: VertexId) -> impl Iterator<Item = &Edge> {
        self.edge_map
            .get(&vertex_id)
            .into_iter()
            .flat_map(|edges| edges.iter())
            .filter(move |e| e.0 == vertex_id)
    }

    /// An iterator visiting all the inbound edges of `vertex_id`.
    /// The iterator element type is `&Edge`
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// g.add_edge(Edge(VertexId(1), VertexId(3)));
    /// g.add_edge(Edge(VertexId(2), VertexId(3)));
    ///
    /// for &e in g.inbound_edges(VertexId(3)) {
    ///     println!("{:?}", e);
    /// }
    ///
    /// ```
    pub fn inbound_edges(&self, vertex_id: VertexId) -> impl Iterator<Item = &Edge> {
        self.edge_map
            .get(&vertex_id)
            .into_iter()
            .flat_map(|edges| edges.iter())
            .filter(move |e| e.1 == vertex_id)
    }

    /// Returns the number of outbound edges of `vertex_id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// assert_eq!(g.degree_out(VertexId(1)), 0);
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// assert_eq!(g.degree_out(VertexId(1)), 1);
    /// ```
    pub fn degree_out(&self, vertex_id: VertexId) -> usize {
        self.outbound_edges(vertex_id).count()
    }

    /// Returns the number of inbound edges of `vertex_id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// assert_eq!(g.degree_in(VertexId(1)), 0);
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// assert_eq!(g.degree_in(VertexId(2)), 1);
    /// ```
    pub fn degree_in(&self, vertex_id: VertexId) -> usize {
        self.inbound_edges(vertex_id).count()
    }


    /// Adds a vertex to the graph. Returns true if the graph already contained `vertex_id `.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::VertexId;
    ///
    /// let mut g = DirectedGraph::new();
    /// assert!(!g.add_vertex(VertexId(1)));
    /// assert!(g.add_vertex(VertexId(1)));
    /// ```
    pub fn add_vertex(&mut self, vertex_id: VertexId) -> bool {
        let mut contains_vertex = true;
        self.edge_map
            .entry(vertex_id)
            .or_insert_with(|| {
                contains_vertex = false;
                BTreeBag::new()
            });
        contains_vertex
    }

    /// Removes a vertex from the graph.
    /// Returns true if the graph contained `vertex_id` before the removal.
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::VertexId;
    ///
    /// let mut g = DirectedGraph::new();
    /// g.add_vertex(VertexId(1));
    /// assert!(g.remove_vertex(VertexId(1)));
    /// assert!(!g.remove_vertex(VertexId(1)));
    /// ```
    pub fn remove_vertex(&mut self, vertex_id: VertexId) -> bool {
        if let Some(edges) = self.edge_map.remove(&vertex_id) {
            // We need to remove all edges containing the vertex
            for &edge in edges.iter() {
                let Edge(v1, v2) = edge;
                if v1 != vertex_id {
                    self.edge_map
                        .get_mut(&v1)
                        .map(|v1_edges| v1_edges.remove(&edge));
                }
                if v2 != vertex_id {
                    self.edge_map
                        .get_mut(&v2)
                        .map(|v2_edges| v2_edges.remove(&edge));
                }
            }
            true
        } else {
            false
        }
    }

    /// Adds an edge to the graph.
    ///
    /// # Examples
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// ```
    pub fn add_edge(&mut self, edge: Edge) {
        let Edge(v1, v2) = edge;
        self.add_vertex(v1);
        self.add_vertex(v2);
        self.edge_map.get_mut(&v1).unwrap().insert(edge);
        if edge.0 != edge.1 {
            self.edge_map.get_mut(&v2).unwrap().insert(edge);
        }
    }

    /// Removes and edge from the graph.
    /// Returns true if that graph contained the `edge` before the removal.
    ///
    /// # Examples
    ///
    /// ```
    /// use histo_graph_core::graph::directed_graph::DirectedGraph;
    /// use histo_graph_core::graph::graph::{VertexId, Edge};
    ///
    /// let mut g = DirectedGraph::new();
    /// g.add_edge(Edge(VertexId(1), VertexId(2)));
    /// assert!(g.remove_edge(Edge(VertexId(1), VertexId(2))));
    /// assert!(!g.remove_edge(Edge(VertexId(1), VertexId(2))));
    /// ```
    pub fn remove_edge(&mut self, edge: Edge) -> bool {
        let Edge(v1, v2) = edge;
        let mut found = false;
        if let Some(found_v1) = self.edge_map.get_mut(&v1) {
            found |= found_v1.remove(&edge);
        }
        if let Some(found_v2) = self.edge_map.get_mut(&v2) {
            found |= found_v2.remove(&edge);
        }
        found
    }
}

impl Hash for DirectedGraph {

    /// Hashes the `DirectedGraph`.
    /// It does so by putting the elements of the underlying HashMap into a `BTreeMap`, which
    /// implements `Hash`.
    fn hash<H: Hasher>(&self, state: &mut H) {
        let vertex_b_tree_map: BTreeMap<&VertexId, &BTreeBag<Edge>> =
          self.edge_map.iter().collect();

        vertex_b_tree_map.hash(state);
    }
}

#[cfg(test)]
mod test {
    use super::DirectedGraph;
    use crate::graph::graph::{Edge, VertexId};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn test_hash() {
        let mut graph = DirectedGraph::new();
        graph.add_edge(Edge(VertexId(0), VertexId(1)));
        graph.add_edge(Edge(VertexId(1), VertexId(2)));

        let mut hasher = DefaultHasher::new();
        graph.hash(&mut hasher);

        let hash_code_1 = hasher.finish();

        let mut graph = DirectedGraph::new();
        // mind the changed order
        graph.add_edge(Edge(VertexId(0), VertexId(1)));
        graph.add_edge(Edge(VertexId(1), VertexId(2)));

        let mut hasher = DefaultHasher::new();
        graph.hash(&mut hasher);

        let hash_code_2 = hasher.finish();

        assert_eq!(hash_code_1, hash_code_2);
    }

    #[test]
    fn test_ne() {
        let mut graph_1 = DirectedGraph::new();
        graph_1.add_vertex(VertexId(0));
        graph_1.add_vertex(VertexId(1));
        graph_1.add_vertex(VertexId(2));

        graph_1.add_edge(Edge(VertexId(0), VertexId(1)));
        graph_1.add_edge(Edge(VertexId(0), VertexId(2)));
        graph_1.add_edge(Edge(VertexId(0), VertexId(0)));

        let mut graph_2 = DirectedGraph::new();
        graph_2.add_vertex(VertexId(0));
        graph_2.add_vertex(VertexId(1));
        graph_2.add_vertex(VertexId(2));

        graph_2.add_edge(Edge(VertexId(0), VertexId(0)));
        graph_2.add_edge(Edge(VertexId(0), VertexId(0)));
        graph_2.add_edge(Edge(VertexId(0), VertexId(1)));
        graph_2.add_edge(Edge(VertexId(0), VertexId(2)));
        graph_2.add_edge(Edge(VertexId(0), VertexId(2)));
        graph_2.add_edge(Edge(VertexId(0), VertexId(1)));

        assert_ne!(graph_1, graph_2);
    }
}