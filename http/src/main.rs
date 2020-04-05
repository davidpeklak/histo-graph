use warp::Filter;
use histo_graph_file::file_storage::*;
use std::path::{PathBuf, Path};
use histo_graph_serde::directed_graph_serde::DirectedGraphSer;
use histo_graph_core::graph::graph::{VertexId, Edge};

#[tokio::main]
async fn main() {
    // get, /show
    let show =
        warp::get()
            .and(warp::path("show"))
            .and_then(fn_show);

    // post, /add-vertex/:vertex_id
    let add_vertex =
        warp::post()
            .and(warp::path("add-vertex"))
            .and(warp::path::param::<u64>())
            .and_then(fn_add_vertex);

    // post, /add-edge/:vertex_id/:vertex_id
    let add_edge =
        warp::post()
            .and(warp::path("add-edge"))
            .and(warp::path::param::<u64>())
            .and(warp::path::param::<u64>())
            .and_then(fn_add_edge);

    let all =
        show
            .or(add_vertex)
            .or(add_edge);

    warp::serve(all).run(([127, 0, 0, 1], 3030)).await;
}

async fn fn_show() -> Result<impl warp::Reply, std::convert::Infallible> {
    let base_dir: PathBuf = Path::new(".store/").into();
    let name = "current".to_string();

    let graph = load_graph(base_dir, name).await.unwrap();
    let ser: DirectedGraphSer = (&graph).into();
    Ok(warp::reply::json(&ser))
}

async fn fn_add_vertex(vertex_id: u64) -> Result<impl warp::Reply, std::convert::Infallible> {
    let base_dir: PathBuf = Path::new(".store/").into();
    let name = "current".to_string();

    let vertex_id = VertexId(vertex_id);

    let mut graph = load_graph(base_dir.clone(), name.clone()).await.unwrap();

    graph.add_vertex(vertex_id);

    save_graph_as(base_dir, name, &graph).await.unwrap();
    Ok(warp::reply::reply())
}

async fn fn_add_edge(vertex_id_from: u64, vertex_id_to: u64) -> Result<impl warp::Reply, std::convert::Infallible> {
    let base_dir: PathBuf = Path::new(".store/").into();
    let name = "current".to_string();

    let edge = Edge(VertexId(vertex_id_from), VertexId(vertex_id_to));

    let mut graph = load_graph(base_dir.clone(), name.clone()).await.unwrap();

    graph.add_edge(edge);

    save_graph_as(base_dir, name, &graph).await.unwrap();
    Ok(warp::reply::reply())
}