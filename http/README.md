# histo-graph-http
An http server to work with [histo-graph](../).

# Usage
First, install the [refajo](../refajo/) sub-module as described [here](../README.md).

Initialize a graph:
```bash
> refajo init
Running sub-command 'init'
```
Then add a few vertices and edges, run
```bash
> refajo --help
```
to see how.

Run the http server
```bash
> cargo run
```

Show the graph [http://127.0.0.1:3030](http://127.0.0.1:3030)
