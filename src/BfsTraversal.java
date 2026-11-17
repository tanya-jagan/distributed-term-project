import java.util.*;
import java.util.function.Consumer;

/** Standard BFS baseline with duplicate detection for each level */
public final class BfsTraversal implements Traversal {
    private static final class StateKey {
        private final int[] state;
        StateKey(int[] state) { this.state = state.clone(); }
        public boolean equals(Object other) {
            return other instanceof StateKey
                && Arrays.equals(state, ((StateKey) other).state);
        }
        public int hashCode() { return Arrays.hashCode(state); }
    }
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        List<int[]> current = new ArrayList<>();
        current.add(new int[trace.processes()]);
        while (!current.isEmpty()) {
            List<int[]> next = new ArrayList<>();
            Set<StateKey> seen = new HashSet<>();
            for (int[] state : current) {
                visit.accept(state.clone());
                for (int p = 0; p < trace.processes(); p++) {
                    if (!trace.enabled(state, p)) continue;
                    int[] successor = state.clone();
                    successor[p]++;
                    if (seen.add(new StateKey(successor))) next.add(successor);
                }
            }
            current = next;
        }
    }
}
