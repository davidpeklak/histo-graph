use clap::{App, SubCommand, Arg};
use histo_graph_file::file_storage::*;
use std::path::{PathBuf, Path};
use histo_graph_serde::directed_graph_serde::DirectedGraphSer;
use tokio::runtime::Runtime;
use error::Result;
use histo_graph_core::graph::directed_graph::DirectedGraph;
use histo_graph_core::graph::graph::{VertexId, Edge};
use futures::future::Future;

mod error;

fn main() -> Result<()> {
    let matches = App::new("histo-graph")
        .version("0.1.0")
        .about("Historizes graphs")
        .subcommand(SubCommand::with_name("init")
            .about("initializes a new graph"))
        .subcommand(SubCommand::with_name("show")
            .about("shows a graph")
            )
        .subcommand(SubCommand::with_name("add-vertex")
            .about("adds a vertex")
            .arg(Arg::with_name("vertexId")
                .required(true)
                .index(1))
        )
        .subcommand(SubCommand::with_name("add-edge")
            .about("adds an edge")
            .arg(Arg::with_name("vertexId_from")
                .required(true)
                .index(1))
            .arg(Arg::with_name("vertexId_to")
                .required(true)
                .index(2))
        )
        .get_matches();

    let base_dir: PathBuf = Path::new(".store/").into();
    let name = "current".to_string();

    if let Some(_) = matches.subcommand_matches("show") {
        println!("Running sub-command 'show' ");

        let f = load_graph(base_dir, name);

        let mut rt = Runtime::new()?;
        let graph = rt.block_on(f)?;
        let ser: DirectedGraphSer = (&graph).into();
        let str = serde_json::to_string(&ser)?;
        println!("{}", str);

        return Ok(());
    }

    if let Some(_) = matches.subcommand_matches("init") {
        println!("Running sub-command 'init' ");

        let graph = DirectedGraph::new();

        let f = save_graph_as(base_dir, name, &graph);

        let mut rt = Runtime::new()?;
        rt.block_on(f)?;

        return Ok(());
    }

    if let Some(matches) = matches.subcommand_matches("add-vertex") {
        println!("Running sub-command 'add-vertex' ");
        if let Some(vertex_id) = matches.value_of("vertexId") {
            println!("Adding vertex '{}'", vertex_id);

            let vertex_id: u64 = std::str::FromStr::from_str(vertex_id)?;
            let vertex_id = VertexId(vertex_id);

            let f = load_graph(base_dir.clone(), name.clone())
                .and_then(move |mut graph| {
                    graph.add_vertex(vertex_id);
                    Ok(graph)
                })
                .and_then({ let name = name.clone(); move |graph| save_graph_as(base_dir, name, &graph)});

            let mut rt = Runtime::new()?;
            rt.block_on(f)?;
        }

        return Ok(());
    }

    if let Some(matches) = matches.subcommand_matches("add-edge") {
        println!("Running sub-command 'add-edge' ");
        if let (Some(vertex_id_from), Some(vertex_id_to)) = (matches.value_of("vertexId_from"), matches.value_of("vertexId_to")) {
            println!("Adding edge '{}' -> '{}'", vertex_id_from, vertex_id_to);

            let vertex_id_from: u64 = std::str::FromStr::from_str(vertex_id_from)?;
            let vertex_id_to: u64 = std::str::FromStr::from_str(vertex_id_to)?;

            let edge = Edge(VertexId(vertex_id_from),  VertexId(vertex_id_to));

            let f = load_graph(base_dir.clone(), name.clone())
                .and_then(move |mut graph| {
                    graph.add_edge(edge);
                    Ok(graph)
                })
                .and_then({ let name = name.clone(); move |graph| save_graph_as(base_dir, name, &graph)});

            let mut rt = Runtime::new()?;
            rt.block_on(f)?;
        }

        return Ok(());
    }

    Ok(())

}
