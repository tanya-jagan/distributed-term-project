import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;

/** BFS without duplicate-detection set */
public final class ImprovedBfsTraversal implements Traversal {
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        int[][] sigma = trace.topologicalRanks();
        Queue<int[]> queue = new ArrayDeque<>();
        queue.add(new int[trace.processes()]);
        while (!queue.isEmpty()) {
            int[] g = queue.remove();
            visit.accept(g.clone());
            boolean[] maximal = new boolean[trace.processes()];
            for (int q = 0; q < trace.processes(); q++)
                maximal[q] = trace.maximal(g, q);
            for (int p = 0; p < trace.processes(); p++) {
                if (!trace.enabled(g, p)) continue;
                boolean allowed = true;
                for (int q = 0; q < trace.processes(); q++) {
                    // Only the smallest sigma among concurrent maxima wins.
                    if (maximal[q] && trace.concurrentWithFrontier(g, p, q)
                            && sigma[p][g[p]] >= sigma[q][g[q]-1]) {
                        allowed = false;
                        break;
                    }
                }
                if (allowed) {
                    int[] h = g.clone();
                    h[p]++;
                    queue.add(h);
                }
            }
        }
    }
}
