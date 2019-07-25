use std::path::{PathBuf, Path};
use histo_graph_core::graph::graph::VertexId;
use serde::{Serialize, Deserialize};

use crate::Hash;

/// A HashEdge respresents an edge by the hashes of the vertices it is connected to.
#[derive(Serialize, Deserialize)]
pub(crate) struct HashEdge {
    pub(crate) from: Hash,
    pub(crate) to: Hash,
}

#[derive(Serialize)]
pub(crate) struct HashVec<OT>(pub(crate) Vec<Hash>, pub(crate) std::marker::PhantomData<OT>);

#[derive(Serialize)]
pub(crate) struct GraphHash {
    pub(crate) vertex_vec_hash: Hash,
    pub(crate) edge_vec_hash: Hash,
}

pub(crate) trait ObjectType {
    fn sub_dir() -> &'static str;

    fn get_path<P>(base_path: P) -> PathBuf
        where P: AsRef<Path>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(Self::sub_dir())
    }
}

pub(crate) trait NamedObjectType {}

impl ObjectType for VertexId {
    fn sub_dir() -> &'static str {
        "vertex"
    }
}

impl ObjectType for HashEdge {
    fn sub_dir() -> &'static str {
        "edge"
    }
}

impl ObjectType for HashVec<VertexId> {
    fn sub_dir() -> &'static str {
        "vertexvec"
    }
}

impl ObjectType for HashVec<HashEdge> {
    fn sub_dir() -> &'static str { "edgevec" }
}

impl ObjectType for GraphHash {
    fn sub_dir() -> &'static str { "graph" }
}

impl NamedObjectType for GraphHash {}