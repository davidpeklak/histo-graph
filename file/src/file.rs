//! This module defines a struct [`File`] that holds all the data that is needed to store an object.
//! It also provides functions to create a `File` from a reference to a type that can be stored,
//! by implementations of `TryInto` for these types.
//! [`File`]: ./struct.File.html

use std::{
    path::{Path, PathBuf},
    convert::TryFrom,
};

use crate::{
    Hash,
    object::{
        ObjectType,
        NamedObjectType,
        HashEdge,
        HashVec,
        GraphHash,
    }
};

use histo_graph_core::graph::graph::{VertexId, Edge};

/// Holds the data that is needed to store an object.
pub(crate) struct File<OT> {

    /// The content to be stored.
    pub(crate) content: Vec<u8>,

    /// The [`Hash`] of the stored content.
    ///
    /// [`Hash`]: ../struct.Hash.html
    pub(crate) hash: Hash,

    _pot: std::marker::PhantomData<OT>,
}

impl<OT> File<OT>
    where OT: ObjectType
{
    pub(crate) fn new(content: Vec<u8>, hash: Hash) -> File<OT> {
        File {
            content,
            hash,
            _pot: std::marker::PhantomData
        }
    }

    /// Returns the directory in which to store objects of type `OT`, given a `base_path`.
    pub(crate) fn create_dir<P>(base_path: P) -> PathBuf
        where P: AsRef<Path>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(OT::storage_name())
    }

    /// Returns the path of the file to be stored, given a `base_path`.
    pub(crate) fn create_path<P>(&self, base_path: P) -> PathBuf
        where P: AsRef<Path>
    {
        File::<OT>::create_path_from_hash(base_path, self.hash)
    }

    /// Returns the path of a file with the given `hash`, under the `base_path`.
    pub(crate) fn create_path_from_hash<P>(base_path: P, hash: Hash) -> PathBuf
        where P: AsRef<Path>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(OT::storage_name()).join(hash.to_string())
    }
}

impl<NOT> File<NOT>
    where NOT: ObjectType,
          NOT: NamedObjectType
{
    /// Returns the path of the file, which is stored under the provided name.
    pub(crate) fn create_named_path<P, S>(&self, base_path: P, name: S) -> PathBuf
        where P: AsRef<Path>,
              S: AsRef<str>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(NOT::storage_name()).join(name.as_ref())
    }
}

impl TryFrom<&VertexId> for File<VertexId> {
    type Error = bincode::Error;

    fn try_from(vertex_id: &VertexId) -> std::result::Result<File<VertexId>, bincode::Error> {
        let content: Vec<u8> = bincode::serialize(&vertex_id.0)?;
        let hash: Hash = (&content).into();

        Ok(File {
            content,
            hash,
            _pot: std::marker::PhantomData,
        })
    }
}

impl TryFrom<&Edge> for File<HashEdge> {
    type Error = bincode::Error;

    fn try_from(edge: &Edge) -> std::result::Result<File<HashEdge>, bincode::Error> {
        let content_from: Vec<u8> = bincode::serialize(&(edge.0).0)?;
        let hash_from: Hash = (&content_from).into();

        let content_to: Vec<u8> = bincode::serialize(&(edge.1).0)?;
        let hash_to: Hash = (&content_to).into();

        let hash_edge = HashEdge { from: hash_from, to: hash_to };

        let content: Vec<u8> = bincode::serialize(&hash_edge)?;
        let hash: Hash = (&content).into();

        Ok(File {
            content,
            hash,
            _pot: std::marker::PhantomData,
        })
    }
}

impl<OT> TryFrom<&HashVec<OT>> for File<HashVec<OT>> {
    type Error = bincode::Error;

    fn try_from(hash_vec: &HashVec<OT>) -> std::result::Result<File<HashVec<OT>>, bincode::Error> {
        let content: Vec<u8> = bincode::serialize(hash_vec)?;
        let hash: Hash = (&content).into();

        Ok(File {
            content,
            hash,
            _pot: std::marker::PhantomData,
        })
    }
}

impl TryFrom<&GraphHash> for File<GraphHash> {
    type Error = bincode::Error;

    fn try_from(graph_hash: &GraphHash) -> std::result::Result<File<GraphHash>, bincode::Error> {
        let content: Vec<u8> = bincode::serialize(graph_hash)?;
        let hash: Hash = (&content).into();

        Ok(File {
            content,
            hash,
            _pot: std::marker::PhantomData,
        })
    }
}

impl TryFrom<&File<VertexId>> for VertexId {
    type Error = bincode::Error;

    fn try_from(file: &File<VertexId>) -> std::result::Result<VertexId, bincode::Error> {
        let id: u64 = bincode::deserialize(file.content.as_ref())?;
        Ok(VertexId(id))
    }
}

impl TryFrom<&File<HashEdge>> for HashEdge {
    type Error = bincode::Error;

    fn try_from(file: &File<HashEdge>) -> Result<HashEdge,bincode::Error> {
        bincode::deserialize::<HashEdge>(file.content.as_ref())
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryInto;
    use histo_graph_core::graph::graph::VertexId;
    use crate::file::File;

    #[test]
    fn test_vertex_id_to_file() -> Result<(), bincode::Error> {
        let vertex_id = VertexId(27u64);
        let file: File<VertexId> = (&vertex_id).try_into()?;
        let result: VertexId = (&file).try_into()?;

        Ok(assert_eq!(vertex_id, result))
    }
}