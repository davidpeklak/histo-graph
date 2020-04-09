# Histo-Graph
[![Build Status](https://travis-ci.org/davidpeklak/histo-graph.svg?branch=master)](https://travis-ci.org/davidpeklak/histo-graph)

A project to store historized graphs.

## Get started
* Clone the repository, and install the [refajo](refajo/) sub-module:
```bash
> cargo install --path refajo
```
* Add the cargo installation directory (most likely `~/.cargo/bin/`) to the `PATH`
environment variable
* Initialize and manipulate a graph with the `refajo` command-line-tool
```bash
> refajo init
Running sub-command 'init'
> refajo add-vertex 1
Running sub-command 'add-vertex' 
Adding vertex '1'
> refajo add-edge 2 3
Running sub-command 'add-edge' 
Adding edge '2' -> '3'
> refajo show
Running sub-command 'show' 
{"vertices":[2,3,1],"edges":[[2,3]]}
```

## Project Structure

### [refajo](refajo/)
A command-line-tool to manipulate a stored graph.

### [histo-graph-http](http/)
An http server to show and manipulate a stored graph.

### [histo-graph-core](core/)
Holds the core data-structures for commands and graphs.

### [histo-graph-serde](serde/)
Implements serialization and deserialization of the core data-structures.

### [histo-graph-file](file/)
Implements a historized file-storage for graphs.
