import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** ParaMount lexical traversal that emits runs along one coordinate. */
public final class BatchedParaMountTraversal implements Traversal {
    private static final class Partition {
        final int[] lower;
        final int[] upper;

        Partition(int[] lower, int[] upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final int threads;

    public BatchedParaMountTraversal(int threads) {
        if (threads < 1) throw new IllegalArgumentException("threads must be positive");
        this.threads = threads;
    }

    @Override
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        int n = trace.processes();
        long eventCountLong = trace.totalEvents();
        if (eventCountLong > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Too many events for ParaMount partition indexing");
        int eventCount = (int) eventCountLong;

        // Visit zero cut
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

    private static void boundedLex(Trace trace, int[] lower, int[] upper,
                                   Consumer<int[]> visit) {
        int n = trace.processes();
        int[] g = lower.clone();
        int previousK = n;
        int last = n - 1;
        while (last >= 0 && lower[last] == upper[last]) last--;
        if (last < 0) {
            visit.accept(g);
            return;
        }

        while (leq(g, upper)) {
            int start = g[last];
            int end = batchEnd(trace, g, last, upper[last]);
            while (true) {
                visit.accept(g.clone());
                if (g[last] == end) break;
                g[last]++;
            }
            if (end > start) previousK = last;
            if (Arrays.equals(g, upper)) return;

            int k = last - 1;
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

    /** Largest legal count on process when all other coordinates stay constant */
    private static int batchEnd(Trace trace, int[] cut, int process, int ceiling) {
        int start = cut[process];
        if (start == ceiling || !trace.enabled(cut, process)) return start;

        // Testing a candidate uses the existing vector-clock check for next event
        cut[process] = ceiling - 1;
        if (trace.enabled(cut, process)) {
            cut[process] = start;
            return ceiling;
        }

        int low = start + 1; // known legal
        int high = ceiling - 1;
        while (low < high) {
            int mid = low + (high - low + 1) / 2;
            cut[process] = mid - 1;
            if (trace.enabled(cut, process)) low = mid;
            else high = mid - 1;
        }
        cut[process] = start;
        return low;
    }

    private static boolean leq(int[] a, int[] b) {
        for (int i = 0; i < a.length; i++) if (a[i] > b[i]) return false;
        return true;
    }
}
