use histo_graph_core::graph::{
    graph::{VertexId, Edge},
    directed_graph::DirectedGraph,
};

use crate::error::{Error, Result};

use ring::digest::{Context, SHA256};
use data_encoding::HEXLOWER;
use serde::{Serialize, Deserialize};

use futures::future::Future;
use std::{
    borrow::Borrow,
    io,
    path::{Path, PathBuf},
};
use std::ffi::OsStr;
use std::convert::{TryFrom, TryInto};


#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {
    fn to_string(&self) -> String {
        HEXLOWER.encode(&self.0)
    }
}

impl<T> From<T> for Hash
    where T: AsRef<[u8]> {
    fn from(content: T) -> Hash {
        let mut context = Context::new(&SHA256);
        context.update(content.as_ref());
        let digest = context.finish();
        let mut hash: [u8; 32] = [0u8; 32];
        hash.copy_from_slice(digest.as_ref());

        Hash(hash)
    }
}

/// A HashEdge respresents an edge by the hashes of the vertices it is connected to.
#[derive(Serialize, Deserialize)]
struct HashEdge {
    from: Hash,
    to: Hash,
}

struct HashVec<OT>(Vec<Hash>, std::marker::PhantomData<OT>);

trait ObjectType {
    fn sub_dir() -> &'static str;

    fn get_path<P>(base_path: P) -> PathBuf
        where P: AsRef<Path>
    {
        let path_buf: PathBuf = base_path.as_ref().into();
        path_buf.join(Self::sub_dir())
    }
}

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

struct File<OT> {
    content: Vec<u8>,
    hash: Hash,
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


    fn write_file<P>(self, base_path: &P) -> impl Future<Item=(), Error=io::Error>
        where P: AsRef<Path>
    {
        let path: PathBuf = self.create_path(base_path);
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
        let hash_to: Hash = (&content_from).into();

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

fn to_file_vec<I, T, OT>(i: I) -> Vec<Result<File<OT>>>
    where I: IntoIterator<Item=T>,
          T: TryInto<File<OT>, Error=bincode::Error>,
          OT: ObjectType
{
    let files: Vec<Result<File<OT>>> = i
        .into_iter()
        .map(TryInto::<File<OT>>::try_into)
        .map(|r| r.map_err(Into::into))
        .collect();

    files
}

fn write_all_files<P, OT>(base_path: P, files: Vec<Result<File<OT>>>) -> impl Future<Item=HashVec<OT>, Error=Error>
    where OT: ObjectType,
          P: AsRef<Path>,
          P: Clone
{
    fn write_one_file<P, OT>(base_path: P, file: File<OT>) -> impl Future<Item=Hash, Error=Error>
        where OT: ObjectType,
              P: AsRef<Path>
    {
        let hash = file.hash;
        file.write_file(&base_path)
            .map_err(Into::into)
            .map(move |_| hash)
    }

    let base_path: PathBuf = base_path.as_ref().into();
    let base_path_clone = base_path.clone();

    let futs = files
        .into_iter()
        .map(futures::done)
        .map(move |fut| fut.and_then({
            let base_path = base_path.clone();
            move |file| write_one_file(base_path, file)
        }));


    tokio_fs::create_dir_all(OT::get_path(base_path_clone))
        .map_err(Into::into)
        .and_then(|_| futures::future::join_all(futs))
        .map(|vec| HashVec(vec, std::marker::PhantomData))
}

fn write_graph_vertices<P>(base_path: P, graph: &DirectedGraph) -> impl Future<Item=HashVec<VertexId>, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    let files = to_file_vec(graph.vertices());

    write_all_files(base_path, files)
}

fn write_graph_edges<P>(base_path: P, graph: &DirectedGraph) -> impl Future<Item=HashVec<HashEdge>, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    let files = to_file_vec(graph.edges());

    write_all_files(base_path, files)
}

