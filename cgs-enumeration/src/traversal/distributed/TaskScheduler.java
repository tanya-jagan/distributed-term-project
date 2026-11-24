package traversal.distributed;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Decides which task a worker gets next; returns null when the worker has no more work. */
public interface TaskScheduler {

	Task next(int workerId);

	/**
	 * Static assignment: the task list is dealt out cyclically up front (task i
	 * goes to worker i mod W). A worker that finishes early stays idle.
	 */
	static TaskScheduler statik(List<Task> tasks, int workers) {
		final ArrayDeque<Task>[] queues = newQueues(workers);
		for (int i = 0; i < tasks.size(); ++i) queues[i % workers].add(tasks.get(i));
		return new TaskScheduler() {
			@Override
			public Task next(int workerId) {
				synchronized (queues[workerId]) {
					return queues[workerId].poll();
				}
			}
		};
	}

	/** Dynamic load balancing: workers pull the next task from one shared, ordered queue. */
	static TaskScheduler dynamic(final List<Task> tasks) {
		final AtomicInteger next = new AtomicInteger();
		return new TaskScheduler() {
			@Override
			public Task next(int workerId) {
				int i = next.getAndIncrement();
				return i < tasks.size() ? tasks.get(i) : null;
			}
		};
	}

	@SuppressWarnings("unchecked")
	static ArrayDeque<Task>[] newQueues(int n) {
		ArrayDeque<Task>[] q = new ArrayDeque[n];
		for (int i = 0; i < n; ++i) q[i] = new ArrayDeque<Task>();
		return q;
	}
}
