package traversal.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Collector for summary mode: workers do not ship their cuts, only
 * (count, hash, first, last) per task. The coordinator then checks that the
 * tasks' [first,last] ranges are pairwise disjoint; if they are, the tasks are
 * lex-contiguous ranges and the concatenation of their sequences, in order of
 * first cut, is globally sorted and duplicate-free, with a combined hash that is
 * comparable to the hash of the centralized output.
 * <p>
 * If the ranges interleave (ParaMount partitions) the order cannot be verified
 * from summaries; only the total count is meaningful then.
 */
public class SummaryVerifier implements ResultCollector {
	private final List<Task> tasks;
	private final boolean countOnly;
	private final TaskSummary[] summaries;
	private boolean finished;
	private boolean orderVerified;
	private long count;
	private long hash;

	public SummaryVerifier(List<Task> tasks) {
		this(tasks, false);
	}

	/** @param countOnly workers sent no digests (COUNT mode): only the total count is available. */
	public SummaryVerifier(List<Task> tasks, boolean countOnly) {
		this.tasks = tasks;
		this.countOnly = countOnly;
		this.summaries = new TaskSummary[tasks.size()];
	}

	@Override
	public synchronized void taskStarted(Task task) { }

	@Override
	public synchronized void chunk(int taskId, int[] cuts, int count) {
		throw new IllegalStateException("summary mode received cuts for task " + taskId);
	}

	@Override
	public synchronized void taskFinished(int taskId, TaskSummary summary) {
		if (summaries[taskId] != null) throw new IllegalStateException("task " + taskId + " finished twice");
		if (summary.violations != 0)
			throw new IllegalStateException("task " + taskId + " reported " + summary.violations
					+ " invalid cuts (unordered or inconsistent)");
		summaries[taskId] = summary;
	}

	@Override
	public synchronized void finish() {
		List<TaskSummary> nonEmpty = new ArrayList<TaskSummary>();
		count = 0;
		for (int i = 0; i < summaries.length; ++i) {
			if (summaries[i] == null) throw new IllegalStateException("task " + i + " did not finish");
			count += summaries[i].count;
			if (countOnly) continue;
			if (summaries[i].count > 0) {
				if (Cuts.compare(summaries[i].first, tasks.get(i).lower) < 0)
					throw new IllegalStateException("task " + i + " produced a cut below its lower bound");
				nonEmpty.add(summaries[i]);
			}
		}
		Collections.sort(nonEmpty, new Comparator<TaskSummary>() {
			@Override
			public int compare(TaskSummary a, TaskSummary b) {
				return Cuts.compare(a.first, b.first);
			}
		});
		orderVerified = !countOnly;
		hash = 0;
		for (int i = 0; i < nonEmpty.size(); ++i) {
			if (i > 0 && Cuts.compare(nonEmpty.get(i - 1).last, nonEmpty.get(i).first) >= 0) orderVerified = false;
			hash = Cuts.concat(hash, nonEmpty.get(i).hash, nonEmpty.get(i).count);
		}
		if (!orderVerified) hash = 0;
		finished = true;
	}

	public synchronized long totalCount() {
		requireFinished();
		return count;
	}

	/** True when the tasks' ranges are disjoint, i.e. count and digest describe a verified global order. */
	public synchronized boolean isOrderVerified() {
		requireFinished();
		return orderVerified;
	}

	/** Hash of the globally ordered sequence; only valid if {@link #isOrderVerified()}. */
	public synchronized long digest() {
		requireFinished();
		return hash;
	}

	private void requireFinished() {
		if (!finished) throw new IllegalStateException("finish() was not called");
	}
}
