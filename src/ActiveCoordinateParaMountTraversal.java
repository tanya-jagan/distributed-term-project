import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** ParaMount lexical traversal that skips fixed coordinates within each interval. */
public final class ActiveCoordinateParaMountTraversal implements Traversal {
    private static final class Partition {
        final int[] lower;
        final int[] upper;

        Partition(int[] lower, int[] upper) {
            this.lower = lower;
            this.upper = upper;
        }
    }

    private final int threads;

    public ActiveCoordinateParaMountTraversal(int threads) {
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

        // The zero cut is not owned by an event interval, so visit it once.
        visit.accept(new int[n]);
        if (eventCount == 0) return;

        // Read each immutable event clock once. The traversal reuses these
        // defensive copies instead of requesting a fresh copy on every check.
        int[][][] clocks = new int[n][][];
        for (int p = 0; p < n; p++) {
            clocks[p] = new int[trace.size(p)][];
            for (int k = 0; k < clocks[p].length; k++) {
                clocks[p][k] = trace.eventClock(p, k);
            }
        }

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
            int[] lower = clocks[p][k];
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
                    boundedLex(clocks, part.lower, part.upper, visit);
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

    private static void boundedLex(int[][][] clocks, int[] lower, int[] upper,
                                   Consumer<int[]> visit) {
        int[] active = new int[lower.length];
        int activeCount = 0;
        for (int p = 0; p < lower.length; p++) {
            if (lower[p] != upper[p]) active[activeCount++] = p;
        }

        int[] cut = lower.clone();
        int previous = activeCount;
        while (true) {
            visit.accept(cut.clone());

            int position = activeCount - 1;
            for (; position >= 0; position--) {
                int process = active[position];
                if (cut[process] < upper[process]
                        && enabled(clocks[process][cut[process]], process, cut, active, activeCount)) {
                    break;
                }
            }
            if (position < 0) {
                if (!leq(upper, cut)) {
                    throw new IllegalStateException("No lexical successor before interval end");
                }
                return;
            }

            int process = active[position];
            cut[process]++;
            if (position == activeCount - 1) {
                previous = position;
                continue;
            }
            if (previous <= position) {
                int[] advanced = clocks[process][cut[process] - 1];
                for (int i = position + 1; i < activeCount; i++) {
                    int suffixProcess = active[i];
                    cut[suffixProcess] = Math.max(cut[suffixProcess], advanced[suffixProcess]);
                }
            } else {
                for (int i = position + 1; i < activeCount; i++) {
                    int suffixProcess = active[i];
                    cut[suffixProcess] = lower[suffixProcess];
                }
                for (int i = 0; i <= position; i++) {
                    int prefixProcess = active[i];
                    if (cut[prefixProcess] == 0) continue;
                    int[] prefixClock = clocks[prefixProcess][cut[prefixProcess] - 1];
                    for (int j = position + 1; j < activeCount; j++) {
                        int suffixProcess = active[j];
                        cut[suffixProcess] = Math.max(cut[suffixProcess], prefixClock[suffixProcess]);
                    }
                }
            }
            previous = position;
        }
    }

    private static boolean enabled(int[] clock, int owner, int[] cut, int[] active, int count) {
        // At a fixed coordinate, cut equals the consistent upper bound,
        // so every dependency of a candidate below that bound is satisfied.
        for (int i = 0; i < count; i++) {
            int process = active[i];
            if (process != owner && clock[process] > cut[process]) return false;
        }
        return true;
    }

    private static boolean leq(int[] a, int[] b) {
        for (int i = 0; i < a.length; i++) if (a[i] > b[i]) return false;
        return true;
    }
}
