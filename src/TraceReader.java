import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class TraceReader {
    private TraceReader() {}

    public static Trace read(Path path) throws IOException {
        List<List<int[]>> chains = null;
        int lineNumber = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                int open = line.indexOf('(');
                int colon = line.indexOf(':');
                if (open < 0 || colon < 0) continue;
                int close = line.indexOf(')', open);
                try {
                    if (close < 0 || colon > open)
                        throw new IllegalArgumentException("Expected pid:(clock)");
                    int pid = Integer.parseInt(line.substring(0, colon).trim());
                    String[] parts = line.substring(open+1, close).split(",", -1);
                    int[] clock = new int[parts.length];
                    for (int j = 0; j < parts.length; j++)
                        clock[j] = Integer.parseInt(parts[j].trim());
                    if (chains == null) {
                        chains = new ArrayList<>();
                        for (int j = 0; j < clock.length; j++) chains.add(new ArrayList<>());
                    }
                    if (clock.length != chains.size() || pid < 0 || pid >= chains.size())
                        throw new IllegalArgumentException("Invalid process ID or clock width");
                    chains.get(pid).add(clock);
                } catch (IllegalArgumentException ex) {
                    throw new IOException(path + ":" + lineNumber + ": " + ex.getMessage(), ex);
                }
            }
        }
        if (chains == null) throw new IOException("No event lines found in " + path);
        int[][][] events = new int[chains.size()][][];
        for (int p = 0; p < chains.size(); p++)
            events[p] = chains.get(p).toArray(new int[0][]);
        try {
            return new Trace(events);
        } catch (IllegalArgumentException ex) {
            throw new IOException("Invalid vector-clock trace: " + ex.getMessage(), ex);
        }
    }
}
