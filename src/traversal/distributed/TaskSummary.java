package traversal.distributed;

/** What a worker reports when it has finished enumerating one task. */
public final class TaskSummary {
	public final long count;
	/** Order-dependent hash of the task's cuts (see {@link Cuts#append}). */
	public final long hash;
	/** First and last cut, null when the task produced no cut. */
	public final int[] first;
	public final int[] last;
	/** Cuts that were not strictly increasing, or inconsistent when validation is on. */
	public final long violations;

	public TaskSummary(long count, long hash, int[] first, int[] last, long violations) {
		this.count = count;
		this.hash = hash;
		this.first = first;
		this.last = last;
		this.violations = violations;
	}
}
