package traversal.distributed;

/**
 * Receives worker results on the coordinator. Implementations are thread-safe:
 * the coordinator calls them from one thread per worker connection.
 */
public interface ResultCollector {

	/** A task was handed to a worker. */
	void taskStarted(Task task);

	/** The next {@code count} cuts of a task, flattened (count * width ints), in lex order. */
	void chunk(int taskId, int[] cuts, int count);

	void taskFinished(int taskId, TaskSummary summary);

	/** Called once all workers are done; throws IllegalStateException if the result is incomplete or inconsistent. */
	void finish();
}
