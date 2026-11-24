package traversal.distributed;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;

/**
 * Streaming k-way merge of per-task sorted streams into one globally
 * lexicographically ordered, duplicate-free stream.
 * <p>
 * Correctness rests on two facts. Every task streams its cuts in increasing lex
 * order, and the first cut of a task is its lower bound (the least consistent cut
 * of the interval is also its lexicographic minimum). So the smallest buffered
 * head {@code c} can be emitted as soon as
 * <ol>
 * <li>no running task has an empty buffer (it could still deliver something
 * smaller than {@code c}), and</li>
 * <li>every task that has not started yet has a lower bound greater than
 * {@code c}.</li>
 * </ol>
 * Emitted cuts must be strictly increasing; an equal or smaller cut means a cut
 * was produced twice or out of order and the merge fails.
 * <p>
 * It works for any disjoint partitioning: lex-contiguous ranges degenerate into
 * concatenation, ParaMount intervals into a real merge.
 */
public class OrderedMerger implements ResultCollector {

	/** Receives the cut that starts at data[off] (width clocks), in global order. */
	public interface CutSink {
		void accept(int[] data, int off);
	}

	private static final int PENDING = 0, RUNNING = 1, DONE = 2;

	private static final class Chunk {
		final int[] data;
		final int length; // ints in use

		Chunk(int[] data, int length) {
			this.data = data;
			this.length = length;
		}
	}

	private static final class State {
		final Task task;
		int status = PENDING;
		final ArrayDeque<Chunk> queue = new ArrayDeque<Chunk>();
		Chunk cur;
		int pos;
		boolean inHeap;
		long received;

		State(Task task) {
			this.task = task;
		}
	}

	private final int width;
	private final CutSink sink;
	private final State[] states;
	private final PriorityQueue<State> heap;
	private final TreeSet<State> pending;
	private int runningEmpty = 0;
	private final int[] last;
	private boolean haveLast = false;
	private long emitted = 0, buffered = 0, peakBuffered = 0;
	private long mergeNanos = 0;

	public OrderedMerger(int width, List<Task> tasks, CutSink sink) {
		this.width = width;
		this.sink = sink;
		this.last = new int[width];
		this.states = new State[tasks.size()];
		this.heap = new PriorityQueue<State>(Math.max(1, tasks.size()), new Comparator<State>() {
			@Override
			public int compare(State a, State b) {
				return Cuts.compare(a.cur.data, a.pos, b.cur.data, b.pos, OrderedMerger.this.width);
			}
		});
		this.pending = new TreeSet<State>(new Comparator<State>() {
			@Override
			public int compare(State a, State b) {
				int c = Cuts.compare(a.task.lower, b.task.lower);
				return c != 0 ? c : Integer.compare(a.task.id, b.task.id);
			}
		});
		for (Task t : tasks) {
			if (t.id < 0 || t.id >= states.length || states[t.id] != null)
				throw new IllegalArgumentException("task ids must be 0..k-1 and unique: " + t);
			states[t.id] = new State(t);
			pending.add(states[t.id]);
		}
	}

	@Override
	public synchronized void taskStarted(Task task) {
		State s = state(task.id);
		if (s.status != PENDING) throw new IllegalStateException("task " + task.id + " started twice");
		pending.remove(s);
		s.status = RUNNING;
		++runningEmpty;
		// a started task can only raise the watermark
		drainTimed();
	}

	@Override
	public synchronized void chunk(int taskId, int[] cuts, int count) {
		State s = state(taskId);
		if (s.status != RUNNING) throw new IllegalStateException("cuts for task " + taskId + " in state " + s.status);
		if (count == 0) return;
		s.queue.add(new Chunk(cuts, count * width));
		s.received += count;
		buffered += count;
		if (buffered > peakBuffered) peakBuffered = buffered;
		if (!s.inHeap) {
			--runningEmpty;
			nextChunk(s);
			s.inHeap = true;
			heap.add(s);
		}
		drainTimed();
	}

	@Override
	public synchronized void taskFinished(int taskId, TaskSummary summary) {
		State s = state(taskId);
		if (s.status != RUNNING) throw new IllegalStateException("task " + taskId + " finished in state " + s.status);
		if (summary.count != s.received)
			throw new IllegalStateException("task " + taskId + " reported " + summary.count + " cuts but "
					+ s.received + " were received");
		if (summary.violations != 0)
			throw new IllegalStateException("task " + taskId + " reported " + summary.violations
					+ " invalid cuts (unordered or inconsistent)");
		s.status = DONE;
		if (!s.inHeap) --runningEmpty;
		drainTimed();
	}

	@Override
	public synchronized void finish() {
		if (!pending.isEmpty()) throw new IllegalStateException("tasks never started: " + pending.size());
		for (State s : states)
			if (s.status != DONE) throw new IllegalStateException("task " + s.task.id + " did not finish");
		if (!heap.isEmpty() || buffered != 0)
			throw new IllegalStateException(buffered + " cuts were left unmerged");
	}

	public synchronized long emittedCount() {
		return emitted;
	}

	/** The most cuts that were buffered at once waiting for the merge watermark. */
	public synchronized long peakBufferedCuts() {
		return peakBuffered;
	}

	public synchronized long mergeNanos() {
		return mergeNanos;
	}

	private State state(int id) {
		if (id < 0 || id >= states.length) throw new IllegalStateException("unknown task " + id);
		return states[id];
	}

	private void nextChunk(State s) {
		s.cur = s.queue.poll();
		s.pos = 0;
	}

	private void drainTimed() {
		long t0 = System.nanoTime();
		drain();
		mergeNanos += System.nanoTime() - t0;
	}

	private void drain() {
		while (runningEmpty == 0 && !heap.isEmpty()) {
			State s = heap.peek();
			if (!pending.isEmpty()
					&& Cuts.compare(pending.first().task.lower, 0, s.cur.data, s.pos, width) <= 0) break;
			if (haveLast && Cuts.compare(last, 0, s.cur.data, s.pos, width) >= 0)
				throw new IllegalStateException("cut " + show(s.cur.data, s.pos) + " from task " + s.task.id
						+ " does not follow the previously emitted cut " + show(last, 0)
						+ ": a cut was produced twice or out of order");
			sink.accept(s.cur.data, s.pos);
			System.arraycopy(s.cur.data, s.pos, last, 0, width);
			haveLast = true;
			++emitted;
			--buffered;
			heap.poll();
			s.pos += width;
			if (s.pos >= s.cur.length) nextChunk(s);
			if (s.cur != null) {
				heap.add(s);
			} else {
				s.inHeap = false;
				if (s.status == RUNNING) ++runningEmpty;
			}
		}
	}

	private String show(int[] data, int off) {
		StringBuilder sb = new StringBuilder("(");
		for (int i = 0; i < width; ++i) sb.append(i > 0 ? "," : "").append(data[off + i]);
		return sb.append(")").toString();
	}
}
