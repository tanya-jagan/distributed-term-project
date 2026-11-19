import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Consumer;

/** Enumerates cuts by adding events in increasing topological rank. */
public final class ReverseSearchTraversal implements Traversal {
    private final boolean incremental;

    public ReverseSearchTraversal(boolean incremental) {
        this.incremental = incremental;
    }

    @Override
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        int eventCount = Math.toIntExact(trace.totalEvents());
        int[] cut = new int[trace.processes()];
        int[][] ranks = trace.topologicalRanks();
        int[] processAtRank = new int[eventCount];
        for (int process = 0; process < ranks.length; process++) {
            for (int rank : ranks[process]) {
                processAtRank[rank] = process;
            }
        }

        ReadyEvents ready = incremental ? new ReadyEvents(trace, ranks, eventCount) : null;
        // Each stack level remembers the last child it tried. At a new level,
        // start just after the rank that was added to reach that level.
        int[] lastTried = new int[Math.addExact(eventCount, 1)];
        lastTried[0] = -1;
        int depth = 0;
        visit.accept(cut.clone());

        while (true) {
            int child = ready == null
                    ? nextByScan(trace, ranks, cut, lastTried[depth], eventCount)
                    : ready.nextAfter(lastTried[depth], eventCount);
            if (child == eventCount) {
                if (depth == 0) return;
                int removed = lastTried[--depth];
                cut[processAtRank[removed]]--;
                if (ready != null) ready.undo(removed);
                continue;
            }

            lastTried[depth] = child;
            cut[processAtRank[child]]++;
            if (ready != null) ready.add(child);
            lastTried[++depth] = child;
            visit.accept(cut.clone());
        }
    }

    /** Returns the next child in the same rank order as the incremental variant. */
    private static int nextByScan(Trace trace, int[][] ranks, int[] cut,
                                  int after, int eventCount) {
        int best = eventCount;
        for (int process = 0; process < cut.length; process++) {
            int index = cut[process];
            if (index == ranks[process].length) continue;
            int rank = ranks[process][index];
            if (rank > after && rank < best && trace.enabled(cut, process)) {
                best = rank;
            }
        }
        return best;
    }

    private static final class ReadyEvents {
        private final int[] missing;
        private final int[][] dependents;
        private final TreeSet<Integer> ready = new TreeSet<>();

        ReadyEvents(Trace trace, int[][] ranks, int eventCount) {
            missing = new int[eventCount];
            List<List<Integer>> outgoing = new ArrayList<>(eventCount);
            for (int rank = 0; rank < eventCount; rank++) {
                outgoing.add(new ArrayList<>());
            }
            int[] zeroClock = new int[ranks.length];
            for (int process = 0; process < ranks.length; process++) {
                int[] previous = zeroClock;
                for (int index = 0; index < ranks[process].length; index++) {
                    int rank = ranks[process][index];
                    int[] clock = trace.eventClock(process, index);
                    if (index > 0) {
                        addEdge(outgoing, ranks[process][index - 1], rank);
                    }
                    for (int other = 0; other < ranks.length; other++) {
                        // The local predecessor already requires unchanged
                        // clock entries. A process prefix covers smaller entries.
                        if (other != process && clock[other] > previous[other]) {
                            addEdge(outgoing, ranks[other][clock[other] - 1], rank);
                        }
                    }
                    previous = clock;
                }
            }
            dependents = new int[eventCount][];
            for (int rank = 0; rank < eventCount; rank++) {
                List<Integer> edges = outgoing.get(rank);
                int[] targets = new int[edges.size()];
                for (int i = 0; i < targets.length; i++) {
                    targets[i] = edges.get(i);
                }
                dependents[rank] = targets;
                if (missing[rank] == 0) ready.add(rank);
            }
        }

        private void addEdge(List<List<Integer>> outgoing, int from, int to) {
            outgoing.get(from).add(to);
            missing[to]++;
        }

        int nextAfter(int rank, int noChild) {
            Integer next = ready.higher(rank);
            return next == null ? noChild : next;
        }

        void add(int rank) {
            ready.remove(rank);
            for (int dependent : dependents[rank]) {
                if (--missing[dependent] == 0) ready.add(dependent);
            }
        }

        void undo(int rank) {
            for (int dependent : dependents[rank]) {
                if (missing[dependent]++ == 0) ready.remove(dependent);
            }
            // Backtracking removed all descendants first. This event's own
            // prerequisites are still present, so it becomes ready again.
            ready.add(rank);
        }
    }
}
