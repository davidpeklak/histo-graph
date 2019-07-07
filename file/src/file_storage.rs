use histo_graph_core::graph::graph::VertexId;

use ring::digest::{Context, SHA256};
use data_encoding::HEXLOWER;

use std::path::{Path, PathBuf};
use futures::future::Future;
use std::{
    borrow::Borrow,
    io
};
use histo_graph_core::graph::directed_graph::DirectedGraph;

#[derive(Clone, Copy)]
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

struct File {
    content: Vec<u8>,
    hash: Hash,
}

fn vertex_to_file(vertex_id: &VertexId) -> File {
    // serialize the vertex_id
    let content: Vec<u8> = bincode::serialize(&vertex_id.0).unwrap();
    let hash: Hash = (&content).into();

    File {
        content,
        hash,
    }
}

fn file_to_vertex(file: &File) -> bincode::Result<VertexId> {
    let id: u64 = bincode::deserialize(file.content.as_ref())?;
    Ok(VertexId(id))
}

fn write_file_in_dir(dir_path: &Path, file: File) -> impl Future<Error = io::Error> {
    let path = dir_path.join(&file.hash.to_string());
    tokio_fs::write(path, file.content)
}

fn read_file_in_dir(dir_path: &Path, hash: Hash) -> impl Future<Item = File, Error = io::Error> {
    let path = dir_path.join(hash.to_string());
    tokio_fs::read(path)
        .map( move |content| File {
            content,
            hash
        })
}

fn write_all_vertices_to_files<I>(path: PathBuf, i: I) -> impl Future<Item=Vec<()>, Error = io::Error>
    where I: IntoIterator,
          <I as IntoIterator>::Item: Borrow<VertexId>
{
    let futs = i
        .into_iter()
        .map(| v | vertex_to_file(v.borrow()))
        .map(move | f | write_file_in_dir(path.as_ref(), f).map(| _ | () )) ;

    futures::future::join_all(futs)
}

fn store_graph_vertices(path: PathBuf, graph: &DirectedGraph) -> impl Future<Item=Vec<()>, Error = io::Error> {
    let vertices: Vec<VertexId> = graph
        .vertices()
        .map(| v | *v)
        .collect();

    tokio_fs::create_dir_all(path.clone()).and_then(move | _ | {
        write_all_vertices_to_files(path, vertices)
    })
}

#[cfg(test)]
mod test {
    use histo_graph_core::graph::graph::VertexId;
    use super::{File,
                vertex_to_file,
                write_all_vertices_to_files};
    use futures::future::Future;
    use tokio::runtime::Runtime;
    use std::path::{Path, PathBuf};
    use histo_graph_core::graph::directed_graph::DirectedGraph;
    use crate::file_storage::{store_graph_vertices, write_file_in_dir, read_file_in_dir, file_to_vertex};

    #[test]
    fn test_hash() {
        let File{content: _, hash} = vertex_to_file(&VertexId(27));

        assert_eq!(hash.to_string(), "4d159113222bfeb85fbe717cc2393ee8a6a85b7ce5ac1791c4eade5e3dd6de41")
    }

    #[test]
    fn test_write_and_read_vertex() -> Result<(), std::io::Error> {
        let vertex = VertexId(18);

        let file = vertex_to_file(&vertex);
        let hash = file.hash;

        let path: PathBuf = Path::new("../target/test/store/").into();

        let f = write_file_in_dir(&path, file)
            .and_then(move | _ | read_file_in_dir(&path, hash));

        let mut rt = Runtime::new()?;
        let file = rt.block_on(f)?;

        let result = file_to_vertex(&file).unwrap();

        assert_eq!(result, vertex);

        Ok(())
    }

    #[test]
    fn test_write_vertices() -> Result<(), std::io::Error> {
        let vertices = vec!{VertexId(1), VertexId(2), VertexId(3), VertexId(4)};

        let path: PathBuf = Path::new("../target/test/store/").into();

        let f = write_all_vertices_to_files(path, vertices.into_iter());

        let mut rt = Runtime::new()?;
        rt.block_on(f)?;

        Ok(())
    }

    #[test]
    fn test_store_graph_vertices() -> Result<(), std::io::Error> {
        let mut graph = DirectedGraph::new();
        graph.add_vertex(VertexId(27));
        graph.add_vertex(VertexId(28));
        graph.add_vertex(VertexId(29));

        let path: PathBuf = Path::new("../target/test/store/").into();

        let f = store_graph_vertices(path, &graph);

        let mut rt = Runtime::new()?;
        rt.block_on(f)?;

        Ok(())
    }
}
