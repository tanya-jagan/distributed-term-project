# Five consistent-global-state enumeration algorithms

Independent educational implementations based on Vijay K. Garg,
"Enumerating Global States of a Distributed Computation" (2003).
Java 8+ JDK required. No external dependencies.

## Run (from this directory)

```sh
mkdir -p out
javac -d out src/*.java
java -cp out Main examples/small.poset all
```

Each algorithm should report 7 states for the example.

## Files

- src/BfsTraversal.java: standard BFS with per-level duplicate detection.
- src/ImprovedBfsTraversal.java: Figure 3, sigma-based unique parent rule.
- src/LexicalTraversal.java: Figure 2, lexicographic traversal.
- src/SpaceEfficientBfsTraversal.java: Figure 4, repeated reduced lex traversal.
- src/CompositionBfsTraversal.java: bounded compositions plus consistency checks.
- src/Trace.java: validated, immutable vector clocks and shared operations.
- src/Traversal.java: callback-based common interface.
- src/Benchmark.java: simple single-run timing and counting.
- src/Main.java: runnable example for all five algorithms.

`TraceReader.java` reads the original repository format: zero-based process ID,
a colon, and a comma-separated vector clock in parentheses. Lines without
both a colon and opening parenthesis are ignored. Per-process events must be
listed in local order; clocks include the event itself. The reader does not
insert dummy zero events: state zero is represented implicitly by Trace.

Run one algorithm on a real input:

```sh
java -cp out Main /path/to/posets/d-100 lex
java -cp out Main /path/to/posets/d-100 improved
```

Algorithm choices: bfs, improved, lex, repeated, composition, all.
With a filename but no algorithm, defaults to lex. No arguments runs the tiny
built-in example with lex. Use all only on manageable inputs; BFS may exhaust
memory and the space-efficient methods may be slow. Initial parsing and validation
are excluded from traversal timing. Actual d-* files were not supplied or tested.
This implements the sequential 2003 paper, not ParaMount. Standard BFS is a
baseline; the composition method uses a simple recursive bounded generator,
not an exact transcription of the external Nijenhuis-Wilf generator.

These Java sources have not been compiled or executed in the delivery environment:
only a Java runtime, not javac, was available. Validate output sets, uniqueness,
and ordering on small traces before collecting results.

Single-run example timings are not reliable performance comparisons. Use warm-ups,
repetitions, separate JVMs for peak-memory comparisons, and fixed resource limits.
Timing includes callback array copies, sigma setup in improved BFS, and trace
copying/validation in repeated lexical BFS. State counts use long and may overflow.
