//! Serialization of vertices, edges and graphs, such that [AntV G6] can interpret it.
//! [AntV G6]: https://g6.antv.vision

use serde::Serialize;
use histo_graph_core::graph::directed_graph::DirectedGraph;
use histo_graph_core::graph::graph::{VertexId, Edge};

#[derive(Serialize)]
pub struct VertexG6 {
    id: String,
    label: String,
}

#[derive(Serialize)]
pub struct EdgeG6 {
    source: String,
    target: String,
    label: String,
}

#[derive(Serialize)]
pub struct DirectedGraphG6 {
    nodes: Vec<VertexG6>,
    edges: Vec<EdgeG6>,
}

impl From<&DirectedGraph> for DirectedGraphG6 {
    fn from(graph: &DirectedGraph) -> DirectedGraphG6 {
        DirectedGraphG6 {
            nodes: graph
                .vertices()
                .map(|&VertexId(id)| VertexG6 {
                    id: id.to_string(),
                    label: id.to_string(),
                })
                .collect(),
            edges: graph
                .edges()
                .map(|&Edge(VertexId(id_1), VertexId(id_2))| EdgeG6 {
                    source: id_1.to_string(),
                    target: id_2.to_string(),
                    label: "edge".to_string(),
                })
                .collect()
        }
    }
}
