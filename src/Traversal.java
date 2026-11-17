import java.util.function.Consumer;

public interface Traversal {
    void enumerate(Trace trace, Consumer<int[]> visit);
}
