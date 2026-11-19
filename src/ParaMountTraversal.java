import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * ParaMount-style parallel enumeration. Each event defines one disjoint
 * interval [least cut(event), boundary(event)]; workers enumerate intervals
 * using either bounded lexical traversal (L-Para) or bounded BFS (B-Para).
 */
public final class ParaMountTraversal implements Traversal {
    public enum Subalgorithm { LEX, BFS }

    private static final class Partition {
        final int[] lower;
        final int[] upper;

        Partition(int[] lower, int[] upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final int threads;
    private final Subalgorithm subalgorithm;

    /** Use lexical subtraversal and all processors reported by the JVM. */
    public ParaMountTraversal() {
        this(Runtime.getRuntime().availableProcessors(), Subalgorithm.LEX);
    }

    public ParaMountTraversal(int threads, Subalgorithm subalgorithm) {
        if (threads < 1) throw new IllegalArgumentException("threads must be positive");
        if (subalgorithm == null) throw new NullPointerException("subalgorithm");
        this.threads = threads;
        this.subalgorithm = subalgorithm;
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

        AtomicInteger nextPartition = new AtomicInteger();
        int workerCount = Math.min(threads, partitions.size());
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        List<Future<?>> futures = new ArrayList<>(workerCount);
        for (int w = 0; w < workerCount; w++) {
            futures.add(pool.submit(() -> {
                int index;
                while ((index = nextPartition.getAndIncrement()) < partitions.size()) {
                    Partition part = partitions.get(index);
                    if (subalgorithm == Subalgorithm.LEX) {
                        boundedLex(trace, part.lower, part.upper, visit);
                    } else {
                        boundedBfs(trace, sigma, part.lower, part.upper, visit);
                    }
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

    /** Bounded BFS with the paper's canonical-parent duplicate rule. */
    private static void boundedBfs(Trace trace, int[][] sigma, int[] lower,
                                   int[] upper, Consumer<int[]> visit) {
        int n = trace.processes();
        Deque<int[]> queue = new ArrayDeque<>();
        queue.addLast(lower.clone());

        while (!queue.isEmpty()) {
            int[] g = queue.removeFirst();
            visit.accept(g.clone());
            for (int p = 0; p < n; p++) {
                if (g[p] >= upper[p] || !trace.enabled(g, p)) continue;
                boolean duplicate = false;
                for (int q = 0; q < n; q++) {
                    if (p == q || g[q] <= lower[q]) continue;
                    int fIndex = g[q] - 1;
                    if (sigma[p][g[p]] < sigma[q][fIndex]
                            && trace.concurrentEvents(p, g[p], q, fIndex)) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    int[] successor = g.clone();
                    successor[p]++;
                    queue.addLast(successor);
                }
            }
        }
    }

    private static boolean leq(int[] a, int[] b) {
        for (int i = 0; i < a.length; i++) if (a[i] > b[i]) return false;
        return true;
    }
}
