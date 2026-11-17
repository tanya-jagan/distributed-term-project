/** Single run timing helper**/
public final class Benchmark {
    private Benchmark() {}
    public static void run(String name, Trace trace, Traversal traversal) {
        long[] count = {0};
        long start = System.nanoTime();
        traversal.enumerate(trace, state -> count[0]++);
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("%s: %,d states, %.6f seconds%n", name, count[0], seconds);
    }
}
