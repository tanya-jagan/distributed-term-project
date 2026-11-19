import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * ParaMount with Squire-style include/exclude splits before worker scheduling.
 * The original event intervals and bounded lexical enumerator are retained.
 */
public final class SquireParaMountTraversal implements Traversal {
    private static final class Partition {
        final int[] lower;
        final int[] upper;
        final double score;

        Partition(int[] lower, int[] upper) {
            this.lower = lower;
            this.upper = upper;
            double logVolume = 0.0;
            for (int p = 0; p < lower.length; p++) {
                logVolume += Math.log1p(upper[p] - lower[p]);
            }
            score = logVolume;
        }
    }

    private static final class Candidate {
        final int index;
        final double score;

        Candidate(int index, double score) {
            this.index = index;
            this.score = score;
        }
    }

    private final int threads;
    private final int maxSplits;

    public SquireParaMountTraversal(int threads) {
        this(threads, threads == 1 ? 0 : (int) Math.min(8L * threads, Integer.MAX_VALUE));
    }

    public SquireParaMountTraversal(int threads, int maxSplits) {
        if (threads < 1) throw new IllegalArgumentException("threads must be positive");
        if (maxSplits < 0) throw new IllegalArgumentException("maxSplits must be nonnegative");
        this.threads = threads;
        this.maxSplits = maxSplits;
    }

    @Override
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        int n = trace.processes();
        long eventCountLong = trace.totalEvents();
        if (eventCountLong > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Too many events for ParaMount partition indexing");
        int eventCount = (int) eventCountLong;

        // The zero cut is not owned by an event interval, so visit it once.
        visit.accept(new int[n]);
        if (eventCount == 0) return;

        int[][] sigma = trace.topologicalRanks();
        int[][] eventAtRank = new int[eventCount][2];
        for (int p = 0; p < n; p++) {
            for (int k = 0; k < trace.size(p); k++) {
                eventAtRank[sigma[p][k]][0] = p;
                eventAtRank[sigma[p][k]][1] = k;
            }
        }

        // In the original builder, a node's boundary is the prefix seen up
        // through that node in a topological ordering of the input events.
        int[] prefix = new int[n];
        List<Partition> partitions = new ArrayList<>(eventCount);
        for (int rank = 0; rank < eventCount; rank++) {
            int p = eventAtRank[rank][0];
            int k = eventAtRank[rank][1];
            prefix[p]++;
            int[] lower = trace.eventClock(p, k);
            int[] upper = prefix.clone();
            if (!leq(lower, upper))
                throw new IllegalArgumentException("Event order is not a topological ordering");
            partitions.add(new Partition(lower, upper));
        }

        splitLargest(trace, partitions, maxSplits);

        AtomicInteger nextPartition = new AtomicInteger();
        int workerCount = Math.min(threads, partitions.size());
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        List<Future<?>> futures = new ArrayList<>(workerCount);
        for (int w = 0; w < workerCount; w++) {
            futures.add(pool.submit(() -> {
                int index;
                while ((index = nextPartition.getAndIncrement()) < partitions.size()) {
                    Partition part = partitions.get(index);
                    boundedLex(trace, part.lower, part.upper, visit);
                }
            }));
        }
        pool.shutdown();
        try {
            for (Future<?> future : futures) future.get();
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
            throw new RuntimeException("ParaMount traversal interrupted", e);
        } catch (ExecutionException e) {
            pool.shutdownNow();
            throw new RuntimeException("ParaMount worker failed", e.getCause());
        }
    }

    /** Figure 2's lex successor, restricted to one ParaMount interval. */
    private static void boundedLex(Trace trace, int[] lower, int[] upper,
                                   Consumer<int[]> visit) {
        int n = trace.processes();
        int[] g = lower.clone();
        int previousK = n;

        while (leq(g, upper)) {
            visit.accept(g.clone());
            if (Arrays.equals(g, upper)) return;

            int k = n - 1;
            for (; k >= 0; k--) {
                if (g[k] == upper[k]) continue;
                if (trace.enabled(g, k)) break;
            }
            if (k < 0)
                throw new IllegalStateException("No bounded lexical successor before interval end");

            g[k]++;
            if (previousK <= k) {
                int[] advancedClock = trace.eventClock(k, g[k] - 1);
                for (int i = k + 1; i < n; i++)
                    if (g[i] < advancedClock[i]) g[i] = advancedClock[i];
            } else {
                for (int i = k + 1; i < n; i++) g[i] = lower[i];
                for (int i = k + 1; i < n; i++) {
                    for (int j = 0; j <= k; j++) {
                        if (g[j] > 0) {
                            int dependency = trace.eventClock(j, g[j] - 1)[i];
                            if (g[i] < dependency) g[i] = dependency;
                        }
                    }
                }
            }
            previousK = k;
        }
    }

    private static void splitLargest(Trace trace, List<Partition> partitions, int budget) {
        if (budget == 0) return;
        PriorityQueue<Candidate> candidates = new PriorityQueue<>((a, b) -> {
            int byScore = Double.compare(b.score, a.score);
            return byScore != 0 ? byScore : Integer.compare(a.index, b.index);
        });
        for (int i = 0; i < partitions.size(); i++) {
            enqueue(candidates, i, partitions.get(i));
        }

        for (int i = 0; i < budget && !candidates.isEmpty(); i++) {
            int index = candidates.remove().index;
            Partition part = partitions.get(index);
            int process = widestCoordinate(part);
            int span = part.upper[process] - part.lower[process];
            int pivot = part.lower[process] + (span - 1) / 2 + 1;

            // Include: add the pivot and all its causal predecessors.
            int[] includeLower = trace.eventClock(process, pivot - 1);
            for (int p = 0; p < includeLower.length; p++) {
                includeLower[p] = Math.max(part.lower[p], includeLower[p]);
            }

            // Exclude: keep the largest prefix on each process whose events
            // do not depend on the pivot. This removes the pivot itself too.
            int[] excludeUpper = new int[part.upper.length];
            for (int p = 0; p < excludeUpper.length; p++) {
                excludeUpper[p] = lastWithoutPivot(
                        trace, p, part.lower[p], part.upper[p], process, pivot);
            }

            Partition exclude = new Partition(part.lower, excludeUpper);
            Partition include = new Partition(includeLower, part.upper);
            partitions.set(index, exclude);
            int includeIndex = partitions.size();
            partitions.add(include);
            enqueue(candidates, index, exclude);
            enqueue(candidates, includeIndex, include);
        }
    }

    private static void enqueue(PriorityQueue<Candidate> candidates, int index, Partition part) {
        if (part.score > 0.0) candidates.add(new Candidate(index, part.score));
    }

    private static int widestCoordinate(Partition part) {
        int widest = 0;
        for (int p = 1; p < part.lower.length; p++) {
            if (part.upper[p] - part.lower[p] > part.upper[widest] - part.lower[widest]) {
                widest = p;
            }
        }
        return widest;
    }

    private static int lastWithoutPivot(Trace trace, int process, int low, int high,
                                        int pivotProcess, int pivot) {
        if (process == pivotProcess) return pivot - 1;
        // Clock components are monotone along each process. The lower bound
        // is known safe because it is consistent and excludes the pivot.
        while (low < high) {
            int mid = low + (int) (((long) high - low + 1) / 2);
            if (trace.eventClock(process, mid - 1)[pivotProcess] < pivot) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;
    }

    private static boolean leq(int[] a, int[] b) {
        for (int i = 0; i < a.length; i++) if (a[i] > b[i]) return false;
        return true;
    }
}
