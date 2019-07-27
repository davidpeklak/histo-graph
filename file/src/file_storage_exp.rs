use histo_graph_core::graph::{
    graph::VertexId,
    directed_graph::DirectedGraph,
};

use crate::error::{Error, Result};

use futures::future::Future;
use std::path::{Path, PathBuf};
use std::convert::TryInto;

use crate::{
    Hash,
    object::{
        ObjectType,
        HashVec,
        HashEdge,
        GraphHash,
    },
    file::File,
};


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

fn write_one_file<P, OT>(base_path: P, file: File<OT>) -> impl Future<Item=Hash, Error=Error>
    where OT: ObjectType,
          P: AsRef<Path>
{
    let hash = file.hash;
    file.write_file(&base_path)
        .map_err(Into::into)
        .map(move |_| hash)
}

fn write_all_files<P, OT>(base_path: P, files: Vec<Result<File<OT>>>) -> impl Future<Item=HashVec<OT>, Error=Error>
    where OT: ObjectType,
          P: AsRef<Path>,
          P: Clone
{
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
        .map(HashVec::new)
}

fn write_hash_vec<'a, P, OT>(base_path: P, hash_vec: &'a HashVec<OT>) -> impl Future<Item=Hash, Error=Error>
    where P: AsRef<Path>,
          OT: ObjectType,
          &'a HashVec<OT>: TryInto<File<HashVec<OT>>, Error=bincode::Error>,
          OT: 'a,
          HashVec<OT>: ObjectType,

{
    futures::done(TryInto::<File<HashVec<OT>>>::try_into(hash_vec))
        .map_err(Into::into)
        .and_then(|file| write_one_file(base_path, file))
}

fn write_graph_vertices<P>(base_path: P, graph: &DirectedGraph) -> impl Future<Item=Hash, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    let files: Vec<Result<File<VertexId>>> = to_file_vec(graph.vertices());

    write_all_files(base_path.clone(), files)
        .and_then(move |hash_vec| write_hash_vec(base_path, &hash_vec))
}

fn write_graph_edges<P>(base_path: P, graph: &DirectedGraph) -> impl Future<Item=Hash, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    let files: Vec<Result<File<HashEdge>>> = to_file_vec(graph.edges());

    write_all_files(base_path.clone(), files)
        .and_then(move |hash_vec| write_hash_vec(base_path, &hash_vec))
}

fn write_graph<P>(base_path: P, graph: &DirectedGraph) -> impl Future<Item=GraphHash, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    let vertex_fut = write_graph_vertices(base_path.clone(), graph);
    let edge_fut = write_graph_edges(base_path, graph);

    vertex_fut.join(edge_fut)
        .map(|(vertex_vec_hash, edge_vec_hash)| GraphHash { vertex_vec_hash, edge_vec_hash })
}

pub fn save_graph_as<P>(base_path: P, name: String, graph: &DirectedGraph) -> impl Future<Item=(), Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    write_graph(base_path.clone(), graph)
        .and_then(|graph_hash| TryInto::<File<GraphHash>>::try_into(&graph_hash)
            .map_err(Into::into))
        .and_then(move |file| file.write_named_file(&base_path, name)
            .map_err(Into::into)
        )
}
