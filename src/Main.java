import java.nio.file.Paths;

public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length > 3) {
            System.err.println("Usage: java -cp out Main [trace-file] [bfs|improved|lex|repeated|composition|paramount|paramount-bfs|paramount-active|paramount-batched|paramount-pivot|all] [threads]");
            return;
        }
        String selected = args.length >= 2 ? args[1] : "lex";
        int threads = args.length == 3
            ? Integer.parseInt(args[2])
            : Runtime.getRuntime().availableProcessors();
        if (threads < 1) throw new IllegalArgumentException("threads must be positive");
        String[] names = {"bfs", "improved", "lex", "repeated", "composition",
            "paramount", "paramount-bfs"};
        Traversal[] algorithms = {
            new BfsTraversal(), new ImprovedBfsTraversal(), new LexicalTraversal(),
            new SpaceEfficientBfsTraversal(), new CompositionBfsTraversal(),
            new ParaMountTraversal(threads, ParaMountTraversal.Subalgorithm.LEX),
            new ParaMountTraversal(threads, ParaMountTraversal.Subalgorithm.BFS)
        };
        boolean activeCoordinates = selected.equals("paramount-active");
        boolean batched = selected.equals("paramount-batched");
        boolean pivot = selected.equals("paramount-pivot");
        boolean known = selected.equals("all") || activeCoordinates || batched || pivot;
        for (String name : names) known |= name.equals(selected);
        if (!known) throw new IllegalArgumentException("Unknown algorithm: " + selected);

        // Parsing and initial validation are outside the measured traversal time.
        Trace trace = args.length == 0
            ? new Trace(new int[][][] {{ {1,0}, {2,1} }, { {0,1}, {1,2} }})
            : TraceReader.read(Paths.get(args[0]));
        System.out.printf("Loaded %d processes, %d events%n", trace.processes(), trace.totalEvents());
        if (activeCoordinates) {
            Benchmark.run(selected, trace, new ActiveCoordinateParaMountTraversal(threads));
            return;
        }
        if (batched) {
            Benchmark.run(selected, trace, new BatchedParaMountTraversal(threads));
            return;
        }
        if (pivot) {
            Benchmark.run(selected, trace, new PivotParaMountTraversal(threads));
            return;
        }
        for (int i = 0; i < names.length; i++)
            if (selected.equals("all") || selected.equals(names[i]))
                Benchmark.run(names[i], trace, algorithms[i]);
    }
}
