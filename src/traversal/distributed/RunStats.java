package traversal.distributed;

import java.util.ArrayList;
import java.util.List;

/** Timing and work counters of one distributed run. All times are nanoseconds. */
public class RunStats {

	public static class WorkerStats {
		public int id;
		public String name;
		public long tasks, cuts;
		/** Time inside bounded Lex (including any artificial delay), excluding socket writes. */
		public long computeNanos;
		/** Time the worker spent writing result chunks to the socket. */
		public long sendNanos;
		/** Time between asking for work and receiving an assignment: scheduling latency + coordinator contention. */
		public long waitNanos;
		/** Time between the worker's last DONE and the end of the whole run (waiting for stragglers). */
		public long tailIdleNanos;
		/** Coordinator-side time spent choosing and sending assignments to this worker. */
		public long schedulingNanos;
		public long bytesToWorker, bytesFromWorker;

		public long idleNanos() {
			return waitNanos + tailIdleNanos;
		}
	}

	public String partitioner, schedule, outputMode;
	public int workers;
	public long tasks;
	public long cuts;
	public long partitionNanos;
	/** From the start of the run until every worker is ready: partitioning, launching, connecting, shipping the poset. */
	public long setupNanos;
	/** From the START barrier until every worker is done. */
	public long traverseNanos;
	/** Time the coordinator spent inside the ordered merge (FULL mode). */
	public long mergeNanos;
	public long peakBufferedCuts;
	public final List<WorkerStats> perWorker = new ArrayList<WorkerStats>();

	public long totalNanos() {
		return setupNanos + traverseNanos;
	}

	/** Largest per-worker cut count divided by the mean; 1.0 is perfect balance. */
	public double imbalance() {
		if (perWorker.isEmpty() || cuts == 0) return 1.0;
		long max = 0;
		for (WorkerStats w : perWorker) max = Math.max(max, w.cuts);
		return max / ((double) cuts / perWorker.size());
	}

	public static double ms(long nanos) {
		return nanos / 1e6;
	}

	public String describe() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("partitioner=%s schedule=%s mode=%s workers=%d tasks=%d cuts=%d%n", partitioner,
				schedule, outputMode, workers, tasks, cuts));
		sb.append(String.format("total=%.1fms (setup %.1f + traverse %.1f)  partition=%.1fms  merge=%.1fms  peakBuffered=%d cuts%n",
				ms(totalNanos()), ms(setupNanos), ms(traverseNanos), ms(partitionNanos), ms(mergeNanos),
				peakBufferedCuts));
		sb.append(String.format("%-4s %-22s %7s %12s %10s %10s %10s %10s %10s%n", "id", "worker", "tasks", "cuts",
				"compute", "send", "wait", "tailIdle", "sched"));
		for (WorkerStats w : perWorker)
			sb.append(String.format("%-4d %-22s %7d %12d %9.1fms %9.1fms %9.1fms %9.1fms %9.1fms%n", w.id,
					w.name.length() > 22 ? w.name.substring(0, 22) : w.name, w.tasks, w.cuts, ms(w.computeNanos),
					ms(w.sendNanos), ms(w.waitNanos), ms(w.tailIdleNanos), ms(w.schedulingNanos)));
		return sb.toString();
	}
}
