import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Split the whole lattice on pivot events, then run lexical ParaMount on each branch. */
public final class PivotParaMountTraversal implements Traversal {
    private final int threads;
    private final int splitDepth;

    public PivotParaMountTraversal(int threads) {
        this(threads, 2);
    }

    public PivotParaMountTraversal(int threads, int splitDepth) {
        if (threads < 1) throw new IllegalArgumentException("threads must be positive");
        if (splitDepth < 0 || splitDepth > 30)
            throw new IllegalArgumentException("splitDepth must be between 0 and 30");
        this.threads = threads;
        this.splitDepth = splitDepth;
    }

    @Override
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        int[] upper = new int[trace.processes()];
        for (int p = 0; p < upper.length; p++) upper[p] = trace.size(p);
        List<Job> jobs = new ArrayList<>();
        split(trace, new int[upper.length], upper, splitDepth, jobs);

        if (threads == 1 || jobs.size() == 1) {
            for (Job job : jobs) job.enumerate(visit);
            return;
        }
        int workers = Math.min(threads, jobs.size());
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        AtomicInteger next = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>(workers);
        try {
            for (int w = 0; w < workers; w++) {
                futures.add(pool.submit(() -> {
                    int index;
                    while (!Thread.currentThread().isInterrupted()
                            && (index = next.getAndIncrement()) < jobs.size()) {
                        jobs.get(index).enumerate(visit);
                    }
                }));
            }
            for (Future<?> future : futures) future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Pivot ParaMount interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Pivot ParaMount worker failed", e.getCause());
        } finally {
            pool.shutdownNow();
        }
    }

    private static void split(Trace trace, int[] lower, int[] upper, int depth,
                              List<Job> jobs) {
        int owner = -1;
        int widest = 0;
        for (int p = 0; p < lower.length; p++) {
            int span = upper[p] - lower[p];
            if (span > widest) {
                widest = span;
                owner = p;
            }
        }
        if (depth == 0 || widest <= 1) {
            buildJobs(trace, lower, upper, jobs);
            return;
        }
        int pivot = lower[owner] + (widest - 1) / 2 + 1;
        int[] includeLower = trace.eventClock(owner, pivot - 1);
        int[] excludeUpper = new int[upper.length];
        for (int p = 0; p < upper.length; p++) {
            includeLower[p] = Math.max(lower[p], includeLower[p]);
            excludeUpper[p] = p == owner ? pivot - 1
                    : lastWithoutPivot(trace, p, lower[p], upper[p], owner, pivot);
        }
        split(trace, lower, excludeUpper, depth - 1, jobs);
        // A consistent upper cut containing the pivot already covers its dependencies.
        if (leq(includeLower, upper)) {
            split(trace, includeLower, upper, depth - 1, jobs);
        }
    }

    private static int lastWithoutPivot(Trace trace, int process, int low, int high,
                                        int owner, int pivot) {
        while (low < high) {
            int middle = low + (int) ((1L + high - low) / 2);
            if (trace.eventClock(process, middle - 1)[owner] < pivot) low = middle;
            else high = middle - 1;
        }
        return low;
    }

    private static void buildJobs(Trace trace, int[] lower, int[] upper, List<Job> jobs) {
        int[] active = new int[lower.length];
        int count = 0;
        for (int p = 0; p < lower.length; p++) {
            if (lower[p] < upper[p]) active[count++] = p;
        }
        active = Arrays.copyOf(active, count);
        int[][][] events = new int[count][][];
        for (int p = 0; p < count; p++) {
            int original = active[p];
            events[p] = new int[upper[original] - lower[original]][];
            for (int e = 0; e < events[p].length; e++) {
                int[] clock = trace.eventClock(original, lower[original] + e);
                int[] projected = new int[count];
                for (int q = 0; q < count; q++) {
                    int coordinate = active[q];
                    projected[q] = Math.max(0, clock[coordinate] - lower[coordinate]);
                }
                events[p][e] = projected;
            }
        }

        Trace reduced = new Trace(events);
        // The empty cut of this reduced trace represents the branch floor.
        jobs.add(new Job(events, active, lower, null, null));
        int[][] ranks = reduced.topologicalRanks();
        int[] processAtRank = new int[Math.toIntExact(reduced.totalEvents())];
        for (int p = 0; p < count; p++) {
            for (int rank : ranks[p]) processAtRank[rank] = p;
        }
        int[] prefix = new int[count];
        for (int p : processAtRank) {
            int[] floor = events[p][prefix[p]++];
            jobs.add(new Job(events, active, lower, floor, prefix.clone()));
        }
    }

    private static final class Job {
        private final int[][][] clocks;
        private final int[] active;
        private final int[] originalFloor;
        private final int[] lower;
        private final int[] upper;

        Job(int[][][] clocks, int[] active, int[] originalFloor, int[] lower, int[] upper) {
            this.clocks = clocks;
            this.active = active;
            this.originalFloor = originalFloor;
            this.lower = lower;
            this.upper = upper;
        }

        void enumerate(Consumer<int[]> visit) {
            if (lower == null) {
                visit.accept(originalFloor.clone());
                return;
            }
            boundedLex(clocks, lower, upper, cut -> {
                int[] full = originalFloor.clone();
                for (int p = 0; p < active.length; p++) full[active[p]] += cut[p];
                visit.accept(full);
            });
        }
    }

    /** Figure 2's lex successor, restricted to one ParaMount interval. */
    private static void boundedLex(int[][][] clocks, int[] lower, int[] upper,
                                   Consumer<int[]> visit) {
        int[] varying = new int[lower.length];
        int count = 0;
        for (int p = 0; p < lower.length; p++) {
            if (lower[p] < upper[p]) varying[count++] = p;
        }
        int[] g = lower.clone();
        int previous = count;

        while (!Thread.currentThread().isInterrupted()) {
            visit.accept(g);

            int position = count - 1;
            for (; position >= 0; position--) {
                int p = varying[position];
                if (g[p] < upper[p] && enabled(clocks[p][g[p]], p, g, varying, count)) break;
            }
            if (position < 0) {
                if (!Arrays.equals(g, upper))
                    throw new IllegalStateException("No bounded lexical successor before interval end");
                return;
            }

            g[varying[position]]++;
            // The advance is enabled, so the existing suffix already meets its dependencies.
            // Rebuild the suffix only when moving left in the coordinate order.
            if (previous > position && position + 1 < count) {
                for (int i = position + 1; i < count; i++) {
                    int p = varying[i];
                    g[p] = lower[p];
                }
                // Fixed prefix events are already included in the consistent lower cut.
                for (int j = 0; j <= position; j++) {
                    int p = varying[j];
                    if (g[p] == 0) continue;
                    int[] clock = clocks[p][g[p] - 1];
                    for (int i = position + 1; i < count; i++) {
                        int suffix = varying[i];
                        g[suffix] = Math.max(g[suffix], clock[suffix]);
                    }
                }
            }
            previous = position;
        }
    }

    private static boolean enabled(int[] clock, int owner, int[] cut, int[] varying, int count) {
        // Fixed coordinates match the consistent upper cut and satisfy its dependencies.
        // The caller checks that the next event fits on the advancing process.
        for (int i = 0; i < count; i++) {
            int p = varying[i];
            if (p != owner && clock[p] > cut[p]) return false;
        }
        return true;
    }

    private static boolean leq(int[] a, int[] b) {
        for (int i = 0; i < a.length; i++) if (a[i] > b[i]) return false;
        return true;
    }
}
