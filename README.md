# Consistent Global State Enumeration

This Java project enumerates consistent cuts from vector-clock traces. It
includes five sequential baselines, two ParaMount traversals, and three
ParaMount variants: Active, Batched, and Pivot.

The sequential algorithms draw on Vijay K. Garg’s *Enumerating Global States
of a Distributed Computation* (2003). The parallel baseline follows Yen-Jung
Chang and Vijay K. Garg’s *A Parallel Algorithm for Global States Enumeration
in Concurrent Systems* (2015).

## Build and run

Run these commands from the project root. A JDK with `javac` is required.
This implementation has no external Java dependencies.

```sh
mkdir -p out
javac -d out src/*.java
java -cp out Main
```

With no arguments, `Main` runs Lex on a built-in trace and should report
7 states. To run algorithms on the small example trace:

```sh
java -cp out Main examples/example_1.txt all 1
java -cp out Main examples/example_1.txt paramount-active 1
java -cp out Main examples/example_1.txt paramount-batched 1
java -cp out Main examples/example_1.txt paramount-pivot 1
```

The command format is `java -cp out Main [trace-file] [algorithm] [threads]`.
If the algorithm is omitted, it defaults to `lex`. If the thread count is
omitted, it defaults to the number of processors available to the JVM.

## Algorithms

| Command | Source file | Description |
|---|---|---|
| `bfs` | `BfsTraversal.java` | Breadth-first traversal with per-level deduplication |
| `improved` | `ImprovedBfsTraversal.java` | Breadth-first traversal using a canonical parent for each cut |
| `lex` | `LexicalTraversal.java` | Lexicographic traversal |
| `repeated` | `SpaceEfficientBfsTraversal.java` | Breadth-first enumeration using repeated lexical traversal |
| `composition` | `CompositionBfsTraversal.java` | Bounded compositions with consistency checks |
| `paramount` | `ParaMountTraversal.java` | ParaMount with bounded lexical traversal (L-Para) |
| `paramount-bfs` | `ParaMountTraversal.java` | ParaMount with bounded breadth-first traversal (B-Para) |
| `paramount-active` | `ActiveCoordinateParaMountTraversal.java` | Skips coordinates fixed within each interval |
| `paramount-batched` | `BatchedParaMountTraversal.java` | Groups consecutive advances along the rightmost varying coordinate |
| `paramount-pivot` | `PivotParaMountTraversal.java` | Recursively splits the search space before constructing ParaMount jobs |

`all` runs the five sequential baselines plus `paramount` and
`paramount-bfs`. Run Active, Batched, and Pivot individually as shown above.
Use `all` on small traces because BFS can exhaust memory and some sequential
baselines can take a long time.

## Input and output

`TraceReader.java` reads one event per line in the form `pid:(v1,v2,...)`.
Process IDs start at zero, events on each process must appear in local order,
and each vector clock includes the event itself. `Trace.java` represents the
empty cut implicitly, so input files need no dummy zero events.

`Main.java` selects the traversal, `Traversal.java` defines its callback
interface, and `Benchmark.java` reports the number of cuts and the traversal
time. Input parsing and initial validation are outside the timed run.

## Separate-JVM implementation

The [distributed-lex branch](https://github.com/tanya-jagan/distributed-term-project/tree/distributed-lex)
contains the coordinator and worker implementation used in the separate-JVM
experiments. See that branch’s README for its build and run commands.