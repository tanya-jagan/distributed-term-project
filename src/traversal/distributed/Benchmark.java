package traversal.distributed;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import poset.DefaultPoset;
import poset.PosetParser;
import traversal.concurrent.ConcurrentUnorderedTraverser;
import traversal.distributed.DistributedLexTraverser.OutputMode;
import traversal.distributed.DistributedLexTraverser.Schedule;
import traversal.distributed.partition.LexRangePartitioner;
import traversal.distributed.partition.ParaMountPartitioner;
import traversal.distributed.partition.Partitioner;
import traversal.enumerator.CounterEnumerator;
import traversal.sequential.LexTraverser;

/**
 * Compares centralized Lex, ConcurrentUnordered (threads) and distributed Lex
 * (static and dynamic assignment, separate worker JVMs).
 *
 * <pre>
 * java traversal.distributed.Benchmark --poset FILE [options]
 *   --workers 1,2,4        worker counts for the distributed runs (default 1,2,4)
 *   --threads 1,2,4        thread counts for ConcurrentUnordered (default 1,2,4)
 *   --reps N               repetitions per configuration, the median is reported (default 3)
 *   --granularity G        tasks per worker for lexrange (default 8)
 *   --mode count,summary   output modes of the distributed runs (default count,summary): count = workers ship only
 *                          counts (comparable to ConcurrentUnordered); summary = count + order-dependent digest per task,
 *                          verified against centralized Lex; full = every cut is shipped and merged in order
 *   --paramount            also run the distributed ParaMount partitioning (count only in summary mode)
 *   --straggler-ms MS      add a second scenario in which worker 0 sleeps MS per task
 *   --csv FILE             write every run as CSV
 * </pre>
 */
public class Benchmark {

	static final class Row {
		String config, partitioner = "-", schedule = "-";
		int workers, threads, tasks;
		long cuts;
		double totalMs, setupMs, traverseMs, partitionMs, mergeMs;
		long maxWorkerCuts, minWorkerCuts;
		double imbalance, idleAvgMs, waitAvgMs, tailAvgMs, sendSumMs, schedSumMs;
		long bytes, peakBuffered;
		String verified = "-";
	}

	public static void main(String[] args) throws Exception {
		String file = null, csv = null, mode = "count,summary";
		int[] workerCounts = { 1, 2, 4 }, threadCounts = { 1, 2, 4 };
		int reps = 3, granularity = 8;
		long stragglerMs = 0;
		boolean paramount = false;
		for (int i = 0; i < args.length; ++i) {
			String a = args[i];
			if (a.equals("--paramount")) { paramount = true; continue; }
			if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + a);
			String v = args[++i];
			if (a.equals("--poset")) file = v;
			else if (a.equals("--workers")) workerCounts = ints(v);
			else if (a.equals("--threads")) threadCounts = ints(v);
			else if (a.equals("--reps")) reps = Integer.parseInt(v);
			else if (a.equals("--granularity")) granularity = Integer.parseInt(v);
			else if (a.equals("--mode")) mode = v;
			else if (a.equals("--straggler-ms")) stragglerMs = Long.parseLong(v);
			else if (a.equals("--csv")) csv = v;
			else throw new IllegalArgumentException("unknown option " + a);
		}
		if (file == null) throw new IllegalArgumentException("--poset is required");
		List<OutputMode> modes = new ArrayList<OutputMode>();
		for (String m : mode.split(",")) modes.add(OutputMode.valueOf(m.trim().toUpperCase()));

		DefaultPoset poset = new DefaultPoset();
		new PosetParser().parsePosetFromFile(file, poset.getBuilder());
		System.out.printf("poset %s: %d processes, %d cores available%n", file, poset.width(),
				Runtime.getRuntime().availableProcessors());

		// untimed reference run: count and order-dependent digest
		DigestEnumerator ref = new DigestEnumerator();
		LexTraverser refLex = new LexTraverser();
		refLex.setEnumerator(ref);
		refLex.traverse(poset);
		if (ref.orderViolations() != 0) throw new IllegalStateException("centralized Lex is not strictly increasing?!");
		System.out.printf("reference: %d cuts, digest %s%n%n", ref.count(), Long.toHexString(ref.digest()));

		List<List<Row>> groups = new ArrayList<List<Row>>();
		List<Row> all = new ArrayList<Row>();

		// centralized Lex
		List<Row> lexRuns = new ArrayList<Row>();
		for (int r = 0; r < reps + 1; ++r) { // the first run warms up the JIT and is discarded
			Row row = centralized(poset, ref.count());
			if (r > 0) lexRuns.add(row);
		}
		groups.add(lexRuns);

		// centralized Lex with the same per-cut digest the workers compute in summary mode
		List<Row> digestRuns = new ArrayList<Row>();
		for (int r = 0; r < reps; ++r) digestRuns.add(centralizedDigest(poset, ref));
		groups.add(digestRuns);

		// ConcurrentUnordered
		for (int t : threadCounts) {
			List<Row> runs = new ArrayList<Row>();
			for (int r = 0; r < reps; ++r) runs.add(concurrent(poset, t, ref.count()));
			groups.add(runs);
		}

		// distributed
		List<Partitioner> partitioners = new ArrayList<Partitioner>();
		partitioners.add(new LexRangePartitioner());
		if (paramount) partitioners.add(new ParaMountPartitioner());
		long[] stragglerSettings = stragglerMs > 0 ? new long[] { 0, stragglerMs } : new long[] { 0 };
		for (long straggler : stragglerSettings) {
			for (Partitioner part : partitioners) {
				for (OutputMode outputMode : modes) {
					for (Schedule sched : Schedule.values()) {
						for (int w : workerCounts) {
							List<Row> runs = new ArrayList<Row>();
							for (int r = 0; r < reps; ++r)
								runs.add(distributed(poset, w, sched, part, granularity, outputMode, straggler, ref));
							groups.add(runs);
						}
					}
				}
			}
		}

		List<Row> medians = new ArrayList<Row>();
		for (List<Row> g : groups) {
			all.addAll(g);
			medians.add(median(g));
		}
		printTable(medians, medians.get(0).traverseMs);
		if (csv != null) writeCsv(csv, all);
	}

	private static int[] ints(String s) {
		String[] p = s.split(",");
		int[] r = new int[p.length];
		for (int i = 0; i < p.length; ++i) r[i] = Integer.parseInt(p[i].trim());
		return r;
	}

	private static Row centralized(DefaultPoset poset, long expected) {
		CounterEnumerator counter = new CounterEnumerator();
		LexTraverser lex = new LexTraverser();
		lex.setEnumerator(counter);
		long t0 = System.nanoTime();
		lex.traverse(poset);
		double ms = (System.nanoTime() - t0) / 1e6;
		Row row = new Row();
		row.config = "Lex (centralized)";
		row.workers = 1;
		row.threads = 1;
		row.cuts = counter.getCount();
		row.totalMs = row.traverseMs = ms;
		row.verified = row.cuts == expected ? "count" : "MISMATCH";
		return row;
	}

	private static Row centralizedDigest(DefaultPoset poset, DigestEnumerator ref) {
		DigestEnumerator d = new DigestEnumerator();
		LexTraverser lex = new LexTraverser();
		lex.setEnumerator(d);
		long t0 = System.nanoTime();
		lex.traverse(poset);
		double ms = (System.nanoTime() - t0) / 1e6;
		Row row = new Row();
		row.config = "Lex+digest (centralized)";
		row.workers = 1;
		row.threads = 1;
		row.cuts = d.count();
		row.totalMs = row.traverseMs = ms;
		row.verified = d.count() == ref.count() && d.digest() == ref.digest() ? "digest" : "MISMATCH";
		return row;
	}

	private static Row concurrent(DefaultPoset poset, int threads, long expected) {
		CounterEnumerator counter = new CounterEnumerator();
		ConcurrentUnorderedTraverser t = new ConcurrentUnorderedTraverser();
		t.setPoolSize(threads);
		t.setEnumerator(counter);
		long t0 = System.nanoTime();
		t.traverse(poset);
		double ms = (System.nanoTime() - t0) / 1e6;
		Row row = new Row();
		row.config = "ConcurrentUnordered";
		row.threads = threads;
		row.cuts = counter.getCount();
		row.totalMs = row.traverseMs = ms;
		row.verified = row.cuts == expected ? "count" : "MISMATCH";
		return row;
	}

	private static Row distributed(DefaultPoset poset, int workers, Schedule sched, Partitioner part, int granularity,
			OutputMode mode, long stragglerMs, DigestEnumerator ref) {
		long[] delays = new long[workers];
		delays[0] = stragglerMs;
		DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(workers).setSchedule(sched)
				.setPartitioner(part).setGranularity(granularity).setOutputMode(mode).setTimeoutMillis(120000)
				.setLauncher(new WorkerLauncher.Processes(delays));
		if (mode == OutputMode.FULL) t.setEnumerator(new CounterEnumerator()); // exercises the ordered emit path
		t.traverse(poset);
		RunStats st = t.getStats();

		Row row = new Row();
		row.config = "DistLex[" + mode.name().toLowerCase() + "]" + (stragglerMs > 0 ? " +straggler" + stragglerMs + "ms" : "");
		row.partitioner = part.name();
		row.schedule = sched.name().toLowerCase();
		row.workers = workers;
		row.tasks = (int) st.tasks;
		row.cuts = st.cuts;
		row.totalMs = RunStats.ms(st.totalNanos());
		row.setupMs = RunStats.ms(st.setupNanos);
		row.traverseMs = RunStats.ms(st.traverseNanos);
		row.partitionMs = RunStats.ms(st.partitionNanos);
		row.mergeMs = RunStats.ms(st.mergeNanos);
		row.peakBuffered = st.peakBufferedCuts;
		row.maxWorkerCuts = 0;
		row.minWorkerCuts = Long.MAX_VALUE;
		for (RunStats.WorkerStats w : st.perWorker) {
			row.maxWorkerCuts = Math.max(row.maxWorkerCuts, w.cuts);
			row.minWorkerCuts = Math.min(row.minWorkerCuts, w.cuts);
			row.idleAvgMs += RunStats.ms(w.idleNanos()) / workers;
			row.waitAvgMs += RunStats.ms(w.waitNanos) / workers;
			row.tailAvgMs += RunStats.ms(w.tailIdleNanos) / workers;
			row.sendSumMs += RunStats.ms(w.sendNanos);
			row.schedSumMs += RunStats.ms(w.schedulingNanos);
			row.bytes += w.bytesToWorker + w.bytesFromWorker;
		}
		row.imbalance = st.imbalance();
		if (st.cuts != ref.count()) row.verified = "MISMATCH";
		else if (mode == OutputMode.COUNT) row.verified = "count";
		else if (mode == OutputMode.SUMMARY) {
			SummaryVerifier sv = t.getSummary();
			if (!sv.isOrderVerified()) row.verified = "count";
			else row.verified = sv.digest() == ref.digest() ? "digest" : "MISMATCH";
		} else {
			row.verified = "ordered"; // the merger fails the run on any out-of-order or duplicate cut
		}
		return row;
	}

	/** The run with the median traverse time. */
	private static Row median(List<Row> runs) {
		List<Row> sorted = new ArrayList<Row>(runs);
		Collections.sort(sorted, new java.util.Comparator<Row>() {
			public int compare(Row a, Row b) { return Double.compare(a.traverseMs, b.traverseMs); }
		});
		return sorted.get(sorted.size() / 2);
	}

	private static void printTable(List<Row> rows, double baselineMs) {
		System.out.println("Median of the repetitions. 'traverse' excludes worker JVM startup and poset shipping (see 'setup'); "
				+ "speedup is relative to centralized Lex traverse time.");
		System.out.printf("%-34s %-9s %-8s %4s %4s %6s %12s %9s %9s %7s %8s %9s %8s %8s %8s %8s %9s %s%n", "config",
				"partition", "sched", "wrk", "thr", "tasks", "cuts", "total ms", "trav ms", "speedup", "setup ms",
				"imbalance", "idle ms", "send ms", "sched ms", "merge ms", "MB moved", "verified");
		for (Row r : rows) {
			System.out.printf("%-34s %-9s %-8s %4d %4d %6d %12d %9.1f %9.1f %7.2f %8.1f %9.2f %8.1f %8.1f %8.1f %8.1f %9.2f %s%n",
					r.config, r.partitioner, r.schedule, r.workers, r.threads, r.tasks, r.cuts, r.totalMs, r.traverseMs,
					baselineMs / r.traverseMs, r.setupMs, r.imbalance, r.idleAvgMs, r.sendSumMs, r.schedSumMs, r.mergeMs,
					r.bytes / 1048576.0, r.verified);
		}
		System.out.println("idle ms = mean per worker of (wait for assignments + waiting for stragglers at the end); "
				+ "send/sched = summed over workers.");
	}

	private static void writeCsv(String file, List<Row> rows) throws IOException {
		PrintWriter w = new PrintWriter(file);
		w.println("config,partitioner,schedule,workers,threads,tasks,cuts,total_ms,setup_ms,traverse_ms,partition_ms,merge_ms,"
				+ "max_worker_cuts,min_worker_cuts,imbalance,idle_avg_ms,wait_avg_ms,tail_avg_ms,send_sum_ms,sched_sum_ms,"
				+ "bytes,peak_buffered_cuts,verified");
		for (Row r : rows) {
			w.printf(java.util.Locale.ROOT, "%s,%s,%s,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%.4f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%s%n",
					r.config, r.partitioner, r.schedule, r.workers, r.threads, r.tasks, r.cuts, r.totalMs, r.setupMs,
					r.traverseMs, r.partitionMs, r.mergeMs, r.maxWorkerCuts, r.minWorkerCuts, r.imbalance, r.idleAvgMs,
					r.waitAvgMs, r.tailAvgMs, r.sendSumMs, r.schedSumMs, r.bytes, r.peakBuffered, r.verified);
		}
		w.close();
		System.out.println("wrote " + file);
	}
}
