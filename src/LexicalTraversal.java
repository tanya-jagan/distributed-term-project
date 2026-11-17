import java.util.Arrays;
import java.util.function.Consumer;

public final class LexicalTraversal implements Traversal {
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        int n = trace.processes();
        int[] state = new int[n];
        while (true) {
            visit.accept(state.clone());
            int p = n - 1;
            while (p >= 0 && !trace.enabled(state, p)) p--;
            if (p < 0) return;
            int[] candidate = state.clone();
            candidate[p]++;
            Arrays.fill(candidate, p + 1, n, 0);
            state = trace.leastConsistent(candidate);
        }
    }
}
