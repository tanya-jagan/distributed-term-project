import java.nio.file.Paths;

public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length > 2) {
            System.err.println("Usage: java -cp out Main [trace-file] [bfs|improved|lex|repeated|composition|all]");
            return;
        }
        String selected = args.length == 2 ? args[1] : "lex";
        String[] names = {"bfs", "improved", "lex", "repeated", "composition"};
        Traversal[] algorithms = {
            new BfsTraversal(), new ImprovedBfsTraversal(), new LexicalTraversal(),
            new SpaceEfficientBfsTraversal(), new CompositionBfsTraversal()
        };
        boolean known = selected.equals("all");
        for (String name : names) known |= name.equals(selected);
        if (!known) throw new IllegalArgumentException("Unknown algorithm: " + selected);

        // Parsing and initial validation are outside the measured traversal time.
        Trace trace = args.length == 0
            ? new Trace(new int[][][] {{ {1,0}, {2,1} }, { {0,1}, {1,2} }})
            : TraceReader.read(Paths.get(args[0]));
        System.out.printf("Loaded %d processes, %d events%n", trace.processes(), trace.totalEvents());
        for (int i = 0; i < names.length; i++)
            if (selected.equals("all") || selected.equals(names[i]))
                Benchmark.run(names[i], trace, algorithms[i]);
    }
}
