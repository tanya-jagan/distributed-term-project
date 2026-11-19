# Distributed Lex: Build and Testing Guide

This README documents how to build, run, and test the distributed consistent-cut enumeration implementation. It covers the original traversers, distributed execution modes, partitioning strategies, scheduling policies, and worker scaling experiments.

**Run all commands from the `cgs-enumeration` project root using PowerShell.**

## 1. Build the Project

Compile and package the project:
ant jar


Run this again after modifying the Java source code.

## 2. Run the Original Implementations

These commands provide centralized and multithreaded baselines for comparison.

### Centralized Lex

ant run "-Dtraverser=Lex" "-Dtest=posets/d-100.txt" "-Denumerators=Counter"



ant run "-Dtraverser=Lex" "-Dtest=posets/d-10k.txt" "-Denumerators=Counter"


### Centralized BFS


ant run "-Dtraverser=BFS" "-Dtest=posets/d-10k.txt" "-Denumerators=Counter"


### Concurrent Unordered (8 Threads)


ant run "-Dtraverser=ConcurrentUnordered" "-Dtest=posets/d-10k.txt" "-Dthreads=8" "-Denumerators=Counter"


## 3. Distributed Implementation

The distributed implementation uses a coordinator and independent worker JVM processes communicating over TCP.

### Command Structure


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode count


### Command-Line Options

| Option | Description |
|---|---|
| `--poset` | Input poset file |
| `--workers` | Number of worker processes |
| `--spawn processes` | Launch independent worker JVM processes |
| `--schedule dynamic` | Workers request tasks as they finish |
| `--schedule static` | Assign tasks upfront |
| `--partition lexrange` | Use the new lexicographic range partitioner |
| `--partition paramount` | Use the ParaMount partitioning adapter |
| `--mode count` | Count cuts without transferring individual cuts |
| `--mode summary` | Compute counts and ordering digests |
| `--mode full` | Transfer cuts to the coordinator and merge results |

## 4. Execution Modes

The distributed implementation supports three execution modes.

### Count Mode

Counts consistent cuts without transferring individual cuts to the coordinator.

Useful for:
- Performance benchmarks
- Worker scaling experiments
- Comparing partitioning strategies
- Measuring scheduling overhead

Example:


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode count


**Note:** Count mode does not verify lexicographic ordering.

### Summary Mode

Computes the number of cuts, an order-dependent digest, and the first and last cuts for each task.

Useful for:
- Testing correctness on large datasets
- Checking partition ordering
- Validating lexicographic ranges
- Avoiding the memory and communication costs of transferring every cut

Example:
java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode summary


Look for:

text
summary: cuts=4962876973 orderVerified=true


`orderVerified=true` indicates that the summary-level ordering checks passed. It does not mean every cut was compared against a separately generated centralized enumeration.

### Full Mode

Transfers individual cuts to the coordinator and merges results into global lexicographic order.

Useful for:
- Testing actual result merging
- Checking final enumeration output
- Validating correctness on smaller datasets

Example:


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-100.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode full


**Warning:** Full mode has substantial memory and communication overhead. Don't use on `d-10k`.

## 5. Compare Partitioning Strategies

The distributed implementation supports two partitioning strategies:

- **Lexrange:** New partitioning algorithm that divides the lattice into contiguous lexicographic ranges.
- **ParaMount:** Adapter that reuses the original ParaMount partition boundaries.

Run both with identical configurations to compare performance.

### Lexrange


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode count


### ParaMount


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition paramount `
  --mode count


**Note:** ParaMount partitions may interleave in lexicographic order, so their results cannot necessarily be concatenated directly. Global ordering requires merging.

## 6. Compare Worker Counts

Test how performance changes as the number of worker processes increases.

### 1 Worker


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 1 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode summary


### 2 Workers


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 2 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode count


### 4 Workers


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 4 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode count


### 8 Workers


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode summary


Use the same execution mode when directly comparing runtimes. Summary mode is preferable when ordering validation is also required.

Calculate speedup using:

text
Speedup = Single-worker runtime / Multi-worker runtime


## 7. Compare Static and Dynamic Scheduling

Dynamic scheduling allows workers to request another task after finishing their current one. Static scheduling assigns tasks upfront.

### Dynamic Scheduling


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule dynamic `
  --partition lexrange `
  --mode count


### Static Scheduling


java -cp "bin;lib\*" traversal.distributed.Coordinator `
  --poset posets\d-10k.txt `
  --workers 8 `
  --spawn processes `
  --schedule static `
  --partition lexrange `
  --mode count


Compare total runtime, worker computation time, and tail idle time to determine whether dynamic scheduling improves workload distribution.

## 8. Recommended Correctness Tests

Use smaller datasets for full enumeration and larger datasets for summary validation.

| Test | Dataset | Workers | Partition | Mode |
|---|---|---:|---|---|
| Full enumeration | d-100 | 8 | Lexrange | Full |
| Ordering validation | d-100 | 8 | Lexrange | Summary |
| Large-scale validation | d-10k | 1 | Lexrange | Summary |
| Large-scale validation | d-10k | 8 | Lexrange | Summary |
| Partition comparison | d-10k | 8 | ParaMount | Count |
| Partition comparison | d-10k | 8 | Lexrange | Count |

For correctness, check that:

1. The total number of cuts matches the expected count.
2. No cuts are missing or duplicated when comparing full outputs.
3. The merged output is lexicographically ordered.
4. Summary mode reports `orderVerified=true` for Lexrange.
