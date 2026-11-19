/** Immutable, in-memory trace represented by per-event vector clocks. */
public final class Trace {
    private final int[][][] events;
    public Trace(int[][][] input) {
        events = new int[input.length][][];
        for (int p = 0; p < input.length; p++) {
            events[p] = new int[input[p].length][];
            for (int k = 0; k < input[p].length; k++) {
                int[] clock = input[p][k];
                if (clock.length != input.length || clock[p] != k + 1)
                    throw new IllegalArgumentException("Invalid clock dimensions or local index");
                for (int j = 0; j < input.length; j++) {
                    if (clock[j] < 0 || clock[j] > input[j].length)
                        throw new IllegalArgumentException("Clock outside trace");
                    if (k > 0 && clock[j] < input[p][k-1][j])
                        throw new IllegalArgumentException("Nonmonotone clock");
                }
                events[p][k] = clock.clone();
            }
        }
        // Clocks must include the dependencies of their dependencies.
        for (int[][] process : events) for (int[] clock : process)
            for (int j = 0; j < events.length; j++) if (clock[j] > 0)
                for (int h = 0; h < events.length; h++)
                    if (events[j][clock[j]-1][h] > clock[h])
                        throw new IllegalArgumentException("Clock is not causally closed");
    }
    public int processes() { return events.length; }
    public int size(int p) { return events[p].length; }
    /** Clock of the zero-based event on process p (returned as a defensive copy). */
    public int[] eventClock(int p, int eventIndex) { return events[p][eventIndex].clone(); }
    public boolean enabled(int[] state, int p) {
        if (state[p] >= size(p)) return false;
        int[] clock = events[p][state[p]];
        for (int j = 0; j < processes(); j++)
            if (j != p && clock[j] > state[j]) return false;
        return true;
    }
    public int[] leastConsistent(int[] candidate) {
        int[] result = candidate.clone();
        for (int i = 0; i < processes(); i++) if (candidate[i] > 0) {
            int[] clock = events[i][candidate[i]-1];
            for (int j = 0; j < processes(); j++)
                result[j] = Math.max(result[j], clock[j]);
        }
        return result;
    }
    public long totalEvents() {
        long total = 0;
        for (int[][] process : events) total += process.length;
        return total;
    }
    public static long level(int[] state) {
        long sum = 0;
        for (int value : state) sum += value;
        return sum;
    }
    public Trace reducedToLevel(long level) {
        int[][][] reduced = new int[processes()][][];
        for (int p = 0; p < processes(); p++) {
            int keep = 0;
            while (keep < size(p) && level(events[p][keep]) <= level) keep++;
            reduced[p] = java.util.Arrays.copyOf(events[p], keep);
        }
        return new Trace(reduced);
    }

    // A frontier event is maximal only if no included event succeeds it.
    public boolean maximal(int[] state, int p) {
        if (state[p] == 0) return false;
        for (int q = 0; q < processes(); q++) {
            if (q != p && state[q] > 0
                    && events[q][state[q]-1][p] >= state[p]) return false;
        }
        return true;
    }
    // For an enabled next event e on p, e cannot precede any event in state.
    public boolean concurrentWithFrontier(int[] state, int p, int q) {
        return state[q] > 0 && events[p][state[p]][q] < state[q];
    }
    // Assign sigma ranks by repeatedly executing an enabled next event.
    public int[][] topologicalRanks() {
        int[][] ranks = new int[processes()][];
        for (int p = 0; p < processes(); p++) ranks[p] = new int[size(p)];
        int[] state = new int[processes()];
        long remaining = totalEvents();
        int rank = 0;
        while (remaining > 0) {
            boolean found = false;
            for (int p = 0; p < processes(); p++) {
                if (!enabled(state, p)) continue;
                ranks[p][state[p]++] = rank++;
                remaining--;
                found = true;
                break;
            }
            if (!found) throw new IllegalArgumentException("Cyclic trace");
        }
        return ranks;
    }
    public boolean consistent(int[] state) {
        for (int p = 0; p < processes(); p++) {
            if (state[p] < 0 || state[p] > size(p)) return false;
            if (state[p] == 0) continue;
            for (int j = 0; j < processes(); j++)
                if (events[p][state[p]-1][j] > state[j]) return false;
        }
        return true;
    }

    /** Whether two events are concurrent according to their vector clocks. */
    public boolean concurrentEvents(int p, int pEvent, int q, int qEvent) {
        int[] a = events[p][pEvent];
        int[] b = events[q][qEvent];
        boolean aLeqB = true, bLeqA = true;
        for (int i = 0; i < processes(); i++) {
            if (a[i] > b[i]) aLeqB = false;
            if (b[i] > a[i]) bLeqA = false;
        }
        return !aLeqB && !bLeqA;
    }
}
