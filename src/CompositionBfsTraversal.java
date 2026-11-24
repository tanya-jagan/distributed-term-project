import java.util.function.Consumer;

/** Generate bounded integer compositions for every level */
public final class CompositionBfsTraversal implements Traversal {
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        long[] suffix = new long[trace.processes() + 1];
        for (int p = trace.processes()-1; p >= 0; p--)
            suffix[p] = suffix[p+1] + trace.size(p);
        int[] state = new int[trace.processes()];
        for (long level = 0; level <= suffix[0]; level++)
            generate(trace, 0, level, state, suffix, visit);
    }
    private void generate(Trace trace, int p, long remaining, int[] state,
                          long[] suffix, Consumer<int[]> visit) {
        if (p == trace.processes()) {
            if (remaining == 0 && trace.consistent(state))
                visit.accept(state.clone());
            return;
        }
        int low = (int) Math.max(0L, remaining - suffix[p+1]);
        int high = (int) Math.min((long) trace.size(p), remaining);
        for (int value = low; value <= high; value++) {
            state[p] = value;
            generate(trace, p+1, remaining-value, state, suffix, visit);
        }
    }
}
