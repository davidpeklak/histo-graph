use histo_graph_core::graph::{
    graph::{VertexId, Edge},
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

    let futs = files
        .into_iter()
        .map(futures::done)
        .map(move |fut| fut.and_then({
            let base_path = base_path.clone();
            move |file| write_file(base_path, file)
                .map_err(Into::into)
        }));

    futures::future::join_all(futs)
        .map(HashVec::new)
}

fn create_dir_and_write_all_files<P, OT>(base_path: P, files: Vec<Result<File<OT>>>) -> impl Future<Item=HashVec<OT>, Error=Error>
    where OT: ObjectType,
          P: AsRef<Path>,
          P: Clone
{
    tokio_fs::create_dir_all(File::<OT>::create_dir(base_path.clone()))
        .map_err(Into::into)
        .and_then(|_| write_all_files(base_path, files))
}

fn write_object<'a, P, T, OT>(base_path: P, object: &'a T) -> impl Future<Item=Hash, Error=Error>
    where P: AsRef<Path>,
          OT: ObjectType,
          &'a T: TryInto<File<OT>, Error=bincode::Error>
{
    futures::done(TryInto::<File<OT>>::try_into(object))
        .map_err(Into::into)
        .and_then(|file| write_file(base_path, file)
            .map_err(Into::into))
}

fn create_dir_and_write_object<'a, P, T, OT>(base_path: P, object: &'a T) -> impl Future<Item=Hash, Error=Error>
    where P: AsRef<Path>,
          P: Clone,
          OT: ObjectType,
          &'a T: TryInto<File<OT>, Error=bincode::Error>
{
    let f = write_object(base_path.clone(), object);
    tokio_fs::create_dir_all(File::<OT>::create_dir(base_path))
        .map_err(Into::into)
        .and_then(move |_| f)
}

/// Writes the vertices of `graph`.
/// Creates the necessary directories.
fn write_graph_vertices<P>(base_path: P, graph: &DirectedGraph) -> impl Future<Item=Hash, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    let files: Vec<Result<File<VertexId>>> = to_file_vec(graph.vertices());

    create_dir_and_write_all_files(base_path.clone(), files)
        .and_then(move |hash_vec| create_dir_and_write_object(base_path, &hash_vec))
}

fn write_graph_edges<P>(base_path: P, graph: &DirectedGraph) -> impl Future<Item=Hash, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    let files: Vec<Result<File<HashEdge>>> = to_file_vec(graph.edges());

    create_dir_and_write_all_files(base_path.clone(), files)
        .and_then(move |hash_vec| create_dir_and_write_object(base_path, &hash_vec))
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

fn read_file<P, OT>(base_path: P, hash: Hash) -> impl Future<Item=File<OT>, Error=io::Error>
    where OT: ObjectType,
          P: AsRef<Path>
{
    let path: PathBuf = File::<OT>::create_path_from_hash(base_path, hash);
    tokio_fs::read(path)
        .map(move |content| File::<OT>::new(content, hash))
}

fn read_object<P, OT>(base_path: P, hash: Hash) -> impl Future<Item=OT, Error=Error>
    where OT: ObjectType,
          for<'a> &'a File<OT>: TryInto<OT, Error=bincode::Error> /* this is a "higher ranked trait bound" https://doc.rust-lang.org/nomicon/hrtb.html */,
          P: AsRef<Path>
{
    read_file::<P, OT>(base_path, hash)
        .map_err(Into::into)
        .and_then(|file| futures::done((&file).try_into()))
        .map_err(Into::into)
}

fn read_edge<P>(base_path: P, hash: Hash) -> impl Future<Item=Edge, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    read_object::<P, HashEdge>(base_path.clone(), hash)
        .and_then(move |HashEdge { from, to }| {
            let from_fut = read_object::<P, VertexId>(base_path.clone(), from);
            let to_fut = read_object::<P, VertexId>(base_path, to);
            from_fut.join(to_fut)
        })
        .map(|(from, to)| Edge(from, to))
}

fn read_all_objects<P, OT>(base_path: P, hashes: Vec<Hash>) -> impl Future<Item=Vec<OT>, Error=Error>
    where P: AsRef<Path>,
          P: Clone,
          OT: ObjectType,
          for<'a> &'a File<OT>: TryInto<OT, Error=bincode::Error>
{
    let futs = hashes
        .into_iter()
        .map({
            move |hash| read_object::<P, OT>(base_path.clone(), hash)
        });

    futures::future::join_all(futs)
}

fn read_graph_vertices<P>(base_path: P, hash: Hash, mut graph: DirectedGraph) -> impl Future<Item=DirectedGraph, Error=Error>
    where P: AsRef<Path>,
          P: Clone
{
    read_object::<P, HashVec<VertexId>>(base_path.clone(), hash)
        .and_then(|hash_vec| read_all_objects(base_path, hash_vec.0))
        .and_then(|vertices| {
            for v in vertices {
                graph.add_vertex(v);
            }
            Ok(graph)
        })
}

#[cfg(test)]
mod test {
    use histo_graph_core::graph::graph::{VertexId, Edge};
    use std::path::{PathBuf, Path};
    use crate::{
        error::Result,
        file::File,
    };

    use futures::future::Future;
    use tokio::runtime::Runtime;

    use super::{
        write_object,
        read_object,
    };
    use crate::object::HashEdge;
    use crate::file_storage_exp::read_edge;

    #[test]
    fn test_write_read_vertex() -> Result<()> {
        let base_path: PathBuf = Path::new("../target/test/store/").into();

        let vertex = VertexId(27);

        let f = tokio_fs::create_dir_all(File::<VertexId>::create_dir(base_path.clone()))
            .map_err(Into::into)
            .and_then({
                let base_path = base_path.clone();
                move |_| write_object(base_path, &vertex)
            })
            .and_then(move |hash| read_object::<PathBuf, VertexId>(base_path, hash));

        let mut rt = Runtime::new()?;
        let result = rt.block_on(f)?;

        Ok(assert_eq!(vertex, result))
    }

    #[test]
    fn test_write_read_edge() -> Result<()> {
        let base_path: PathBuf = Path::new("../target/test/store/").into();

        let edge = Edge(VertexId(3), VertexId(4));

        let f = tokio_fs::create_dir_all(File::<VertexId>::create_dir(base_path.clone()))
            .and_then({
                let base_path = base_path.clone();
                move |_| tokio_fs::create_dir_all(File::<HashEdge>::create_dir(base_path))
            })
            .map_err(Into::into)
            .and_then({
                let base_path = base_path.clone();
                move |_| {
                    let f_1 = write_object(base_path.clone(), &edge.0);
                    let f_2 = write_object(base_path.clone(), &edge.1);
                    let f_3 = write_object(base_path, &edge);
                    f_1.join3(f_2, f_3)
                }
            })
            .and_then(move |(_, _, hash)| read_edge(base_path, hash));

        let mut rt = Runtime::new()?;
        let result = rt.block_on(f)?;

        Ok(assert_eq!(edge, result))
    }
}