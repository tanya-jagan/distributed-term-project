package traversal.distributed;

import java.io.IOException;

import poset.DefaultPoset;
import poset.PosetParser;
import traversal.distributed.DistributedLexTraverser.OutputMode;
import traversal.distributed.DistributedLexTraverser.Schedule;
import traversal.distributed.partition.LexRangePartitioner;
import traversal.distributed.partition.ParaMountPartitioner;
import traversal.distributed.partition.Partitioner;
import util.SimpleOptions;

/**
 * Command line entry point of the coordinator.
 *
 * <pre>
 * java traversal.distributed.Coordinator --poset FILE [options]
 *   --workers N            number of worker processes to wait for (default 2)
 *   --schedule S           dynamic | static (default dynamic)
 *   --partition P          lexrange | paramount (default lexrange)
 *   --granularity G        tasks per worker for lexrange (default 4)
 *   --mode M               full | summary (default full)
 *   --enumerators E1:E2    existing enumerators, e.g. Counter:Stdout (default none)
 *   --spawn S              processes | threads | none: start the workers itself (default none)
 *   --host H --port P      address to listen on (default 127.0.0.1, port 0 = pick a free port)
 *   --validate             workers check every cut for consistency
 *   --timeout-ms T         wait this long for workers (default 60000)
 * </pre>
 */
public class Coordinator {

	public static void main(String[] args) throws Exception {
		String posetFile = null, enumerators = null, spawn = "none", host = "127.0.0.1";
		int workers = 2, granularity = 4, port = 0;
		long timeout = 60000;
		boolean validate = false;
		Schedule schedule = Schedule.DYNAMIC;
		OutputMode mode = OutputMode.FULL;
		Partitioner partitioner = new LexRangePartitioner();

		for (int i = 0; i < args.length; ++i) {
			String a = args[i];
			if (a.equals("--validate")) {
				validate = true;
				continue;
			}
			if (i + 1 >= args.length) usage("missing value for " + a);
			String v = args[++i];
			if (a.equals("--poset")) posetFile = v;
			else if (a.equals("--workers")) workers = Integer.parseInt(v);
			else if (a.equals("--schedule")) schedule = Schedule.valueOf(v.toUpperCase());
			else if (a.equals("--partition")) partitioner = v.equals("paramount") ? new ParaMountPartitioner()
					: v.equals("lexrange") ? new LexRangePartitioner() : usage("unknown partitioner " + v);
			else if (a.equals("--granularity")) granularity = Integer.parseInt(v);
			else if (a.equals("--mode")) mode = OutputMode.valueOf(v.toUpperCase());
			else if (a.equals("--enumerators")) enumerators = v;
			else if (a.equals("--spawn")) spawn = v;
			else if (a.equals("--host")) host = v;
			else if (a.equals("--port")) port = Integer.parseInt(v);
			else if (a.equals("--timeout-ms")) timeout = Long.parseLong(v);
			else usage("unknown option " + a);
		}
		if (posetFile == null) usage("--poset is required");

		DefaultPoset poset = new DefaultPoset();
		try {
			new PosetParser().parsePosetFromFile(posetFile, poset.getBuilder());
		} catch (IOException e) {
			System.err.println("cannot read poset: " + e);
			System.exit(1);
		}

		DistributedLexTraverser traverser = new DistributedLexTraverser().setWorkers(workers).setSchedule(schedule)
				.setPartitioner(partitioner).setGranularity(granularity).setOutputMode(mode).setValidate(validate)
				.setBindHost(host).setPort(port).setTimeoutMillis(timeout);
		if (spawn.equals("processes")) traverser.setLauncher(new WorkerLauncher.Processes());
		else if (spawn.equals("threads")) traverser.setLauncher(new WorkerLauncher.Threads());
		else if (!spawn.equals("none")) usage("unknown --spawn " + spawn);

		if (enumerators != null) {
			SimpleOptions options = new SimpleOptions();
			options.parse(new String[] { "Lex", posetFile, "1", enumerators });
			traverser.addEnumerator(options.getEnumerator());
		}

		traverser.traverse(poset);

		RunStats stats = traverser.getStats();
		System.out.print(stats.describe());
		if (traverser.getSummary() != null) {
			SummaryVerifier s = traverser.getSummary();
			System.out.println("summary: cuts=" + s.totalCount() + " orderVerified=" + s.isOrderVerified()
					+ (s.isOrderVerified() ? " digest=" + Long.toHexString(s.digest()) : ""));
		}
	}

	private static Partitioner usage(String problem) {
		System.err.println(problem);
		System.err.println("usage: traversal.distributed.Coordinator --poset FILE [--workers N] [--schedule static|dynamic]"
				+ " [--partition lexrange|paramount] [--granularity G] [--mode full|summary] [--enumerators A:B]"
				+ " [--spawn processes|threads|none] [--host H] [--port P] [--validate] [--timeout-ms T]");
		System.exit(2);
		return null;
	}
}
