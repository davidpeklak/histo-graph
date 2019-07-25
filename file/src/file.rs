use std::{
    io,
    path::{Path, PathBuf},
    convert::TryFrom,
};
use futures::future::Future;

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

pub(crate) struct File<OT> {
    content: Vec<u8>,
    pub(crate) hash: Hash,
    _pot: std::marker::PhantomData<OT>,
}

impl<OT> File<OT>
    where OT: ObjectType
{
    fn create_path<P>(&self, base_path: P) -> PathBuf
        where P: AsRef<Path>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(OT::sub_dir()).join(self.hash.to_string())
    }


    pub(crate) fn write_file<P>(self, base_path: &P) -> impl Future<Item=(), Error=io::Error>
        where P: AsRef<Path>
    {
        let path: PathBuf = self.create_path(base_path);
        tokio_fs::write(path, self.content)
            .map(|_| ())
    }
}

impl<NOT> File<NOT>
    where NOT: ObjectType,
          NOT: NamedObjectType
{
    fn create_named_path<P, S>(&self, base_path: P, name: S) -> PathBuf
        where P: AsRef<Path>,
              S: AsRef<str>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(NOT::sub_dir()).join(name.as_ref())
    }

    pub(crate) fn write_named_file<P, S>(self, base_path: &P, name: S) -> impl Future<Item=(), Error=io::Error>
        where P: AsRef<Path>,
              S: AsRef<str>
    {
        let path: PathBuf = self.create_named_path(base_path, name);
        tokio_fs::write(path, self.content)
            .map(|_| ())
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

impl TryFrom<&File<VertexId>> for VertexId {
    type Error = bincode::Error;

    fn try_from(file: &File<VertexId>) -> std::result::Result<Self, Self::Error> {
        let id: u64 = bincode::deserialize(file.content.as_ref())?;
        Ok(VertexId(id))
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