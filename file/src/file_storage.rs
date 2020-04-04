//! Implements the functions that write and read a graph to the file system.

use histo_graph_core::graph::{
    graph::{VertexId, Edge},
    directed_graph::DirectedGraph,
};

use crate::error::Result;

use std::{
    io,
    path::{Path, PathBuf},
};
use std::convert::TryInto;
use tokio::fs;
use futures;

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

/// Takes an interator over objects of type `OT` and returns a vector of `File<OT>`.
fn to_file_vec<I, T, OT>(i: I) -> Result<Vec<File<OT>>>
    where I: IntoIterator<Item=T>,
          T: TryInto<File<OT>, Error=bincode::Error>,
          OT: ObjectType
{
    i
        .into_iter()
        .map(TryInto::<File<OT>>::try_into)
        .map(|r| r.map_err(Into::into))
        .collect()
}

async fn write_file<P, OT>(base_path: P, file: File<OT>) -> std::result::Result<Hash, io::Error>
    where OT: ObjectType,
          P: AsRef<Path>
{
    let path: PathBuf = file.create_path(base_path);
    fs::write(path, file.content).await?;
    Ok(file.hash)
}

async fn write_named_file<P, S, NOT>(base_path: P, name: S, file: File<NOT>) -> std::result::Result<(), io::Error>
    where NOT: ObjectType,
          NOT: NamedObjectType,
          P: AsRef<Path>,
          S: AsRef<str>
{
    let path: PathBuf = File::<NOT>::create_named_path(base_path, name);
    fs::write(path, file.content).await?;
    Ok(())
}

async fn write_all_files<P, OT>(base_path: P, files: Vec<File<OT>>) -> Result<HashVec<OT>>
    where OT: ObjectType,
          P: AsRef<Path>,
          P: Clone
{
    let base_path: PathBuf = base_path.as_ref().into();

    let futs = files
        .into_iter()
        .map(|file| write_file(base_path.clone(), file));

    let vec = futures::future::try_join_all(futs).await?;

    Ok(HashVec::<OT>::new(vec))
}

async fn create_dir_and_write_all_files<P, OT>(base_path: P, files: Vec<File<OT>>) -> Result<HashVec<OT>>
    where OT: ObjectType,
          P: AsRef<Path>,
          P: Clone
{
    fs::create_dir_all(File::<OT>::create_dir(base_path.clone())).await?;
    write_all_files(base_path, files).await
}

async fn write_object<'a, P, T, OT>(base_path: P, object: &'a T) -> Result<Hash>
    where P: AsRef<Path>,
          OT: ObjectType,
          &'a T: TryInto<File<OT>, Error=bincode::Error>
{
    let file = TryInto::<File<OT>>::try_into(object)?;
    Ok(write_file(base_path, file).await?)
}

async fn create_dir_and_write_object<'a, P, T, OT>(base_path: P, object: &'a T) -> Result<Hash>
    where P: AsRef<Path>,
          P: Clone,
          OT: ObjectType,
          &'a T: TryInto<File<OT>, Error=bincode::Error>
{
    fs::create_dir_all(File::<OT>::create_dir(base_path.clone())).await?;
    write_object(base_path, object).await
}

/// Writes the vertices of `graph`.
/// Creates the necessary directories.
async fn write_graph_vertices<P>(base_path: P, graph: &DirectedGraph) -> Result<Hash>
    where P: AsRef<Path>,
          P: Clone
{
    let files = to_file_vec(graph.vertices())?;
    let hash_vec = create_dir_and_write_all_files(base_path.clone(), files).await?;
    create_dir_and_write_object(base_path, &hash_vec).await
}

/// Writes the edges of `graph`.
/// Creates the necessary directories
async fn write_graph_edges<P>(base_path: P, graph: &DirectedGraph) -> Result<Hash>
    where P: AsRef<Path>,
          P: Clone
{
    let files = to_file_vec(graph.edges())?;
    let hash_vec = create_dir_and_write_all_files(base_path.clone(), files).await?;
    create_dir_and_write_object(base_path, &hash_vec).await
}

async fn write_graph<P>(base_path: P, graph: &DirectedGraph) -> Result<GraphHash>
    where P: AsRef<Path>,
          P: Clone
{
    Ok(GraphHash {
        vertex_vec_hash: write_graph_vertices(base_path.clone(), graph).await?,
        edge_vec_hash: write_graph_edges(base_path, graph).await?
    })
}
pub async fn save_graph_as<P>(base_path: P, name: String, graph: &DirectedGraph) -> Result<()>
    where P: AsRef<Path>,
          P: Clone
{
    let graph_hash = write_graph(base_path.clone(), graph).await?;
    let file = TryInto::<File<GraphHash>>::try_into(&graph_hash)?;
    fs::create_dir_all(File::<GraphHash>::create_dir(base_path.clone())).await?;
    Ok(write_named_file(base_path, name, file).await?)
}

async fn read_file<P, OT>(base_path: P, hash: Hash) -> Result<File<OT>>
    where OT: ObjectType,
          P: AsRef<Path>
{
    let path: PathBuf = File::<OT>::create_path_from_hash(base_path, hash);
    Ok(File::<OT>::new(fs::read(path).await?, hash))
}

async fn read_named_file<P, S, NOT>(base_path: P, name: S) -> Result<File<NOT>>
    where NOT: ObjectType,
          NOT: NamedObjectType,
          P: AsRef<Path>,
          S: AsRef<str>
{
    let path: PathBuf = File::<NOT>::create_named_path(base_path, name);
    let content = fs::read(path).await?;
    let hash: Hash = (&content).into();
    Ok(File::<NOT>::new(content, hash))
}

async fn read_object<P, OT>(base_path: P, hash: Hash) -> Result<OT>
    where OT: ObjectType,
          for<'a> &'a File<OT>: TryInto<OT, Error=bincode::Error> /* this is a "higher ranked trait bound" https://doc.rust-lang.org/nomicon/hrtb.html */,
          P: AsRef<Path>
{
    let file:File<OT> = read_file(base_path, hash).await?;
    Ok((&file).try_into()?)
}

async fn read_named_object<P, S, NOT>(base_path: P, name: S) -> Result<NOT>
    where NOT: ObjectType,
          NOT: NamedObjectType,
          for<'a> &'a File<NOT>: TryInto<NOT, Error=bincode::Error> /* this is a "higher ranked trait bound" https://doc.rust-lang.org/nomicon/hrtb.html */,
          P: AsRef<Path>,
          S: AsRef<str>
{
    let file: File<NOT> = read_named_file(base_path, name).await?;
    Ok((&file).try_into()?)
}

async fn read_edge<P>(base_path: P, hash: Hash) -> Result<Edge>
    where P: AsRef<Path>,
          P: Clone
{
    let HashEdge { from, to } = read_object::<P, HashEdge>(base_path.clone(), hash).await?;
    Ok(Edge(
        read_object::<P, VertexId>(base_path.clone(), from).await?,
        read_object::<P, VertexId>(base_path, to).await?
    ))
}

async fn read_all_objects<P, OT>(base_path: P, hashes: Vec<Hash>) -> Result<Vec<OT>>
    where P: AsRef<Path>,
          P: Clone,
          OT: ObjectType,
          for<'a> &'a File<OT>: TryInto<OT, Error=bincode::Error>
{
    let futs = hashes
        .into_iter()
        .map(move |hash| read_object::<P, OT>(base_path.clone(), hash));

    futures::future::try_join_all(futs).await
}

async fn read_all_edges<P>(base_path: P, hashes: Vec<Hash>) -> Result<Vec<Edge>>
    where P: AsRef<Path>,
          P: Clone
{
    let futs = hashes
        .into_iter()
        .map(move |hash| read_edge::<P>(base_path.clone(), hash));

    futures::future::try_join_all(futs).await
}

/// Reads the vertices of a graph.
///
/// Note that this function consumes the graph, and gives it back in the returned Future, with
/// the vertices added.
async fn read_graph_vertices<P>(base_path: P, vertex_vec_hash: Hash, mut graph: DirectedGraph) -> Result<DirectedGraph>
    where P: AsRef<Path>,
          P: Clone
{
    let hash_vec: HashVec<VertexId> = read_object(base_path.clone(), vertex_vec_hash).await?;
    let vertices = read_all_objects(base_path, hash_vec.0).await?;

    for v in vertices {
        graph.add_vertex(v);
    }

    Ok(graph)
}

/// Reads the edges of a graph.
///
/// Note that this function consumes the graph, and gives it back in the returned Future, with
/// the edges added.
async fn read_graph_edges<P>(base_path: P, edge_vec_hash: Hash, mut graph: DirectedGraph) -> Result<DirectedGraph>
    where P: AsRef<Path>,
          P: Clone
{
    let hash_vec: HashVec<HashEdge> = read_object(base_path.clone(), edge_vec_hash).await?;
    let edges = read_all_edges(base_path, hash_vec.0).await?;

    for e in edges {
        graph.add_edge(e);
    }

    Ok(graph)
}

async fn read_graph<P>(base_path: P, graph_hash: &GraphHash) -> Result<DirectedGraph>
    where P: AsRef<Path>,
          P: Clone
{
    let &GraphHash { vertex_vec_hash, edge_vec_hash } = graph_hash;

    let graph = read_graph_vertices(base_path.clone(), vertex_vec_hash, DirectedGraph::new()).await?;
    read_graph_edges(base_path, edge_vec_hash, graph).await
}

pub async fn load_graph<P>(base_path: P, name: String) -> Result<DirectedGraph>
    where P: AsRef<Path>,
          P: Clone
{
    let graph_hash = read_named_object::<P, String, GraphHash>(base_path.clone(), name).await?;
    read_graph(base_path, &graph_hash).await
}

#[cfg(test)]
mod test {
    use histo_graph_core::graph::graph::{VertexId, Edge};
    use std::path::{PathBuf, Path};
    use crate::{
        error::Result,
        file::File,
    };

    use tokio::runtime::Runtime;

    use super::*;
    use crate::object::HashEdge;
    use histo_graph_core::graph::directed_graph::DirectedGraph;

    #[test]
    fn test_write_read_vertex() -> Result<()> {
        let mut rt = Runtime::new()?;
        rt.block_on(async {
            let base_path: PathBuf = Path::new("../target/test/store/").into();

            let vertex = VertexId(27);

            fs::create_dir_all(File::<VertexId>::create_dir(base_path.clone())).await?;

            let hash = write_object(base_path.clone(), &vertex).await?;

            let result = read_object::<PathBuf, VertexId>(base_path, hash).await?;

            Ok(assert_eq!(vertex, result))
        })
    }

    #[test]
    fn test_write_read_edge() -> Result<()> {
        let mut rt = Runtime::new()?;
        rt.block_on(async {
            let base_path: PathBuf = Path::new("../target/test/store/").into();
            let edge = Edge(VertexId(3), VertexId(4));

            fs::create_dir_all(File::<VertexId>::create_dir(base_path.clone())).await?;
            fs::create_dir_all(File::<HashEdge>::create_dir(base_path.clone())).await?;

            let f1 = write_object(base_path.clone(), &edge.0);
            let f2 = write_object(base_path.clone(), &edge.1);
            let f3 = write_object(base_path.clone(), &edge);

            let (_, _, hash) = futures::future::try_join3(f1, f2, f3).await?;

            let result = read_edge(base_path, hash).await?;

            Ok(assert_eq!(edge, result))
        })
    }

    #[test]
    fn test_write_read_graph_vertices() -> Result<()> {
        let mut rt = Runtime::new()?;
        rt.block_on(async {
            let base_path: PathBuf = Path::new("../target/test/store/").into();

            let graph = {
                let mut graph = DirectedGraph::new();
                graph.add_vertex(VertexId(14));
                graph.add_vertex(VertexId(17));
                graph
            };

            let hash = write_graph_vertices(base_path.clone(), &graph).await?;

            let result = read_graph_vertices(base_path, hash, DirectedGraph::new()).await?;

            Ok(assert_eq!(graph, result))
        })
    }

    #[test]
    fn test_write_read_graph() -> Result<()> {
        let mut rt = Runtime::new()?;
        rt.block_on(async {
            let base_path: PathBuf = Path::new("../target/test/store/").into();

            let graph = {
                let mut graph = DirectedGraph::new();
                graph.add_vertex(VertexId(14));
                graph.add_edge(Edge(VertexId(14), VertexId(15)));
                graph
            };

            let hash = write_graph(base_path.clone(), &graph).await?;
            let result = read_graph(base_path, &hash).await?;

            Ok(assert_eq!(graph, result))
        })
    }

    #[test]
    fn test_save_as_and_load_graph() -> Result<()> {
        let mut rt = Runtime::new()?;
        rt.block_on(async {
            let base_path: PathBuf = Path::new("../target/test/store/").into();
            let name = "graph_pepi".to_string();

            let graph = {
                let mut graph = DirectedGraph::new();
                graph.add_vertex(VertexId(19));
                graph.add_edge(Edge(VertexId(12), VertexId(19)));
                graph
            };

            save_graph_as(base_path.clone(), name.clone(), &graph).await?;
            let result = load_graph(base_path, name).await?;

            Ok(assert_eq!(graph, result))
        })
    }

}