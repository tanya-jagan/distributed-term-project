import java.util.concurrent.atomic.LongAdder;


public final class Benchmark {
    private Benchmark() {}
    public static void run(String name, Trace trace, Traversal traversal) {
        LongAdder count = new LongAdder();
        long start = System.nanoTime();
        traversal.enumerate(trace, state -> count.increment());
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("%s: %,d states, %.6f seconds%n", name, count.sum(), seconds);
    }
}
