use histo_graph_core::graph::{
    graph::VertexId,
    directed_graph::DirectedGraph,
};

use crate::error::{Error, Result};

use futures::future::Future;
use std::{
    io,
    path::{Path, PathBuf},
};
use std::convert::TryInto;

use crate::{
    Hash,
    object::{
        ObjectType,
        NamedObjectType,
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

fn write_file<P, OT>(base_path: P, file: File<OT>) -> impl Future<Item=Hash, Error=io::Error>
    where OT: ObjectType,
          P: AsRef<Path>
{
    let path: PathBuf = file.create_path(base_path);
    let hash = file.hash;
    tokio_fs::write(path, file.content)
        .map(move |_| hash)
}

fn write_named_file<P, S, NOT>(base_path: &P, name: S, file: File<NOT>) -> impl Future<Item=(), Error=io::Error>
    where NOT: ObjectType,
          NOT: NamedObjectType,
          P: AsRef<Path>,
          S: AsRef<str>
{
    let path: PathBuf = file.create_named_path(base_path, name);
    tokio_fs::write(path, file.content)
        .map(|_| ())
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
            move |file| write_file(base_path, file)
                .map_err(Into::into)
        }));


    tokio_fs::create_dir_all(File::<OT>::create_dir(base_path_clone))
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
        .and_then(|file| write_file(base_path, file)
            .map_err(Into::into))
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
        .and_then(move |file| write_named_file(&base_path, name, file)
            .map_err(Into::into)
        )
}

fn read_file<P, OT>(base_path: &P, hash: Hash) -> impl Future<Item=File<OT>, Error=io::Error>
    where OT: ObjectType,
          P: AsRef<Path>
{
    let path: PathBuf = File::<OT>::create_path_from_hash(base_path, hash);
    tokio_fs::read(path)
        .map(move |content| File::<OT>::new(content, hash))
}

fn read_object<P, OT>(base_path: &P, hash: Hash) -> impl Future<Item=OT, Error=Error>
    where OT: ObjectType,
          for<'a> &'a File<OT>: TryInto<OT, Error=bincode::Error> /* this is a "higher ranked trait bound" https://doc.rust-lang.org/nomicon/hrtb.html */,
          P: AsRef<Path>
{
    read_file::<P, OT>(base_path, hash)
        .map_err(Into::into)
        .and_then(|file| futures::done((&file).try_into()))
        .map_err(Into::into)
}

#[cfg(test)]
mod test {
    use histo_graph_core::graph::graph::VertexId;

    #[test]
    fn test_write_read_vertex() {
        let vertex = VertexId(27);

    }
}