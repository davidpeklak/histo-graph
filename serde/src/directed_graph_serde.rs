use serde::{Serialize, Deserialize};
use histo_graph_core::graph::directed_graph::DirectedGraph;
use histo_graph_core::graph::graph::{VertexId, Edge};

#[derive(Serialize, Deserialize)]
pub struct DirectedGraphSer {
    vertices: Vec<u64>,
    edges: Vec<(u64, u64)>,
}

impl From<&DirectedGraph> for DirectedGraphSer {
    fn from(graph: &DirectedGraph) -> DirectedGraphSer {
        DirectedGraphSer {
            vertices: graph
                .vertices()
                .map(|&VertexId(id)| id)
                .collect(),
            edges: graph
                .edges()
                .map(|&Edge(VertexId(id_1), VertexId(id_2))| (id_1, id_2))
                .collect()
        }
    }
}

impl From<&DirectedGraphSer> for DirectedGraph {
    fn from(graph_ser: &DirectedGraphSer) -> DirectedGraph {
        let mut graph = DirectedGraph::new();

        for &v_id in &graph_ser.vertices {
            graph.add_vertex(VertexId(v_id));
        }

        for &(v_id_1, v_id_2) in &graph_ser.edges {
            graph.add_edge(Edge(VertexId(v_id_1), VertexId(v_id_2)));
        }

        graph
    }
}

#[cfg(test)]
mod test {
    use histo_graph_core::graph::directed_graph::DirectedGraph;
    use histo_graph_core::graph::graph::{Edge, VertexId};
    use crate::directed_graph_serde::DirectedGraphSer;

    #[test]
    fn test_small() -> Result<(), serde_json::error::Error> {
        let mut graph = DirectedGraph::new();
        graph.add_edge(Edge(VertexId(0), VertexId(1)));
        graph.add_edge(Edge(VertexId(0), VertexId(2)));
        graph.add_edge(Edge(VertexId(0), VertexId(0)));

        let graph_ser: DirectedGraphSer = (&graph).into();

        let str = serde_json::to_string(&graph_ser)?;

        let graph_ser = serde_json::from_str(&str)?;

        let ser_de_graph = DirectedGraph::from(&graph_ser);

        assert_eq!(graph, ser_de_graph);
        Ok(())
    }
}
