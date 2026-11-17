import java.util.function.Consumer;

/** Space-Efficient BFS */
public final class SpaceEfficientBfsTraversal implements Traversal {
    public void enumerate(Trace trace, Consumer<int[]> visit) {
        Traversal lexical = new LexicalTraversal();
        for (long r = 0; r <= trace.totalEvents(); r++) {
            final long target = r;
            Trace reduced = trace.reducedToLevel(r);
            lexical.enumerate(reduced, state -> {
                if (Trace.level(state) == target) visit.accept(state);
            });
        }
    }
}
