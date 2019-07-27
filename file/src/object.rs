//! This module defines the object types that can be stored. It defines the trait [`ObjectType`],
//! which is implemented by all types that represent storable objects.
//!
//! [`ObjectType`]: ./trait.ObjectType.html

use std::path::{PathBuf, Path};
use histo_graph_core::graph::graph::VertexId;
use serde::{Serialize, Deserialize};

use crate::Hash;

/// Respresents an edge by the [`Hash`]es of the vertices it is connected to.
/// This is the type that gets serialized and stored, when storing an edge.
///
/// [`Hash`]: ../struct.Hash.html
#[derive(Serialize, Deserialize)]
pub(crate) struct HashEdge {
    pub(crate) from: Hash,
    pub(crate) to: Hash,
}

/// A vector of [`Hash`]es. These are the `Hash`es of objects that are stored, like `VertexId`s
/// or `HashEdge`s.
///
/// [`Hash`]: ../struct.Hash.html
#[derive(Serialize)]
pub(crate) struct HashVec<OT>(pub(crate) Vec<Hash>, std::marker::PhantomData<OT>);

impl<OT> HashVec<OT>
where OT: ObjectType {

    /// Constructs a `HashVec` where the [`Hash`]es are the ones of objects of type `OT`.
    ///
    /// [`Hash`]: ../struct.Hash.html
    pub(crate) fn new(hashes: Vec<Hash>) -> HashVec<OT> {
        HashVec(hashes, std::marker::PhantomData)
    }
}

/// The top-object of a stored graph.
#[derive(Serialize)]
pub(crate) struct GraphHash {

    /// The [`Hash`] of the [`HashVec`] of the vertices.
    ///
    /// [`Hash`]: ../struct.Hash.html
    /// [`HashVec`]: ./struct.HashVec.html
    pub(crate) vertex_vec_hash: Hash,

    /// The [`Hash`] of the [`HashVec`] of the edges.
    ///
    /// [`Hash`]: ../struct.Hash.html
    /// [`HashVec`]: ./struct.HashVec.html
    pub(crate) edge_vec_hash: Hash,
}

/// Marks types as objects that can be stored.
pub(crate) trait ObjectType {
    fn sub_dir() -> &'static str;

    fn get_path<P>(base_path: P) -> PathBuf
        where P: AsRef<Path>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(Self::sub_dir())
    }
}

/// Marks types as objects that can be stored under a name (rather than storing them by their
/// [`Hash`]).
///
/// [`Hash`]: ../struct.Hash.html
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