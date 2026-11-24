package traversal.distributed;

import static traversal.distributed.TestSuite.check;
import static traversal.distributed.TestSuite.checkEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import poset.DefaultPoset;
import traversal.distributed.partition.LexRangePartitioner;
import traversal.distributed.partition.ParaMountPartitioner;
import traversal.distributed.partition.Partitioner;

/** Tests of the ordered merge, without any networking. */
public class MergerTests {

	private static OrderedMerger merger(final List<int[]> out, Task... tasks) {
		return new OrderedMerger(2, Arrays.asList(tasks), new OrderedMerger.CutSink() {
			public void accept(int[] data, int off) {
				out.add(new int[] { data[off], data[off + 1] });
			}
		});
	}

	private static Task task(int id, int[] lo, int[] hi) {
		return new Task(id, lo, hi);
	}

	private static void chunk(OrderedMerger m, int id, int[]... cuts) {
		int[] flat = new int[cuts.length * 2];
		for (int i = 0; i < cuts.length; ++i) {
			flat[2 * i] = cuts[i][0];
			flat[2 * i + 1] = cuts[i][1];
		}
		m.chunk(id, flat, cuts.length);
	}

	private static TaskSummary summary(int count) {
		return new TaskSummary(count, 0, null, null, 0);
	}

	private static String show(List<int[]> l) {
		return TestKit.show(l);
	}

	private static void expectViolation(Runnable r, String what) {
		try {
			r.run();
		} catch (IllegalStateException e) {
			return;
		}
		throw new AssertionError("expected a violation for: " + what);
	}

	public static void register(TestSuite s) {
		s.add("merger: interleaved streams are emitted only when the watermark allows", new TestSuite.TestBody() {
			public void run() {
				List<int[]> out = new ArrayList<int[]>();
				Task a = task(0, new int[] { 0, 0 }, new int[] { 9, 9 });
				Task b = task(1, new int[] { 1, 0 }, new int[] { 9, 9 });
				OrderedMerger m = merger(out, a, b);
				m.taskStarted(a);
				m.taskStarted(b);
				chunk(m, 1, new int[] { 1, 0 }, new int[] { 3, 0 });
				checkEquals(0, out.size(), "B alone must not be emitted: A may still deliver smaller cuts");
				chunk(m, 0, new int[] { 0, 0 }, new int[] { 2, 0 });
				checkEquals("[0, 0][1, 0][2, 0]", show(out), "after A's first chunk");
				chunk(m, 0, new int[] { 4, 0 });
				checkEquals("[0, 0][1, 0][2, 0][3, 0]", show(out), "(3,0) is below A's new head (4,0)");
				m.taskFinished(0, summary(3));
				checkEquals("[0, 0][1, 0][2, 0][3, 0]", show(out), "B is running and empty: (4,0) must wait");
				m.taskFinished(1, summary(2));
				checkEquals("[0, 0][1, 0][2, 0][3, 0][4, 0]", show(out), "after B finished");
				m.finish();
			}
		});
		s.add("merger: a not-yet-started task with a smaller lower bound holds back emission", new TestSuite.TestBody() {
			public void run() {
				List<int[]> out = new ArrayList<int[]>();
				Task a = task(0, new int[] { 0, 0 }, new int[] { 9, 9 });
				Task b = task(1, new int[] { 1, 0 }, new int[] { 9, 9 });
				OrderedMerger m = merger(out, a, b);
				m.taskStarted(a);
				chunk(m, 0, new int[] { 0, 0 }, new int[] { 2, 0 });
				checkEquals("[0, 0]", show(out), "only cuts below B's lower bound (1,0)");
				m.taskStarted(b);
				chunk(m, 1, new int[] { 1, 0 });
				checkEquals("[0, 0][1, 0]", show(out), "B delivered, A has (2,0) buffered, B now empty-running");
				m.taskFinished(1, summary(1));
				checkEquals("[0, 0][1, 0][2, 0]", show(out), "B finished");
				m.taskFinished(0, summary(2));
				m.finish();
			}
		});
		s.add("merger: duplicate cut from two tasks is detected", new TestSuite.TestBody() {
			public void run() {
				final List<int[]> out = new ArrayList<int[]>();
				final Task a = task(0, new int[] { 0, 0 }, new int[] { 9, 9 });
				final Task b = task(1, new int[] { 0, 0 }, new int[] { 9, 9 });
				final OrderedMerger m = merger(out, a, b);
				m.taskStarted(a);
				m.taskStarted(b);
				chunk(m, 0, new int[] { 0, 0 }, new int[] { 1, 1 });
				expectViolation(new Runnable() {
					public void run() { chunk(m, 1, new int[] { 0, 0 }); }
				}, "duplicate (0,0)");
			}
		});
		s.add("merger: out-of-order cuts inside a task are detected", new TestSuite.TestBody() {
			public void run() {
				final List<int[]> out = new ArrayList<int[]>();
				final Task a = task(0, new int[] { 0, 0 }, new int[] { 9, 9 });
				final OrderedMerger m = merger(out, a);
				m.taskStarted(a);
				expectViolation(new Runnable() {
					public void run() { chunk(m, 0, new int[] { 2, 0 }, new int[] { 1, 0 }); }
				}, "descending cuts");
			}
		});
		s.add("merger: worker-reported count must match the cuts received", new TestSuite.TestBody() {
			public void run() {
				final List<int[]> out = new ArrayList<int[]>();
				final Task a = task(0, new int[] { 0, 0 }, new int[] { 9, 9 });
				final OrderedMerger m = merger(out, a);
				m.taskStarted(a);
				chunk(m, 0, new int[] { 0, 0 });
				expectViolation(new Runnable() {
					public void run() { m.taskFinished(0, summary(2)); }
				}, "count mismatch");
			}
		});
		s.add("merger: empty tasks and tasks that never start are handled", new TestSuite.TestBody() {
			public void run() {
				final List<int[]> out = new ArrayList<int[]>();
				Task empty = task(0, new int[] { 0, 0 }, new int[] { -1, 0 });
				Task real = task(1, new int[] { 0, 0 }, new int[] { 1, 1 });
				final OrderedMerger m = merger(out, empty, real);
				m.taskStarted(empty);
				m.taskFinished(0, summary(0));
				m.taskStarted(real);
				chunk(m, 1, new int[] { 0, 0 }, new int[] { 1, 1 });
				m.taskFinished(1, summary(2));
				m.finish();
				checkEquals("[0, 0][1, 1]", show(out), "output");

				final OrderedMerger incomplete = merger(new ArrayList<int[]>(), task(0, real.lower, real.upper));
				expectViolation(new Runnable() {
					public void run() { incomplete.finish(); }
				}, "finish() with an unstarted task");
			}
		});
		s.add("merger: fuzz - random dispatch order and arrival interleavings equal centralized Lex", new TestSuite.TestBody() {
			public void run() {
				Random rnd = new Random(7);
				for (int seed = 0; seed < 50; ++seed) {
					DefaultPoset p = TestKit.randomPoset(2 + seed % 4, 3 + seed % 3, 0.2 + 0.1 * (seed % 5), 500 + seed);
					Partitioner[] partitioners = { new ParaMountPartitioner(seed % 2 == 0), new LexRangePartitioner() };
					for (Partitioner part : partitioners) {
						List<Task> tasks = part.partition(p, 1 + rnd.nextInt(12));
						checkFuzz(p, tasks, rnd, part.name() + " seed " + seed);
					}
				}
			}
		});
	}

	/** Simulates workers: each task is start, chunks..., finish; events of different tasks interleave randomly. */
	private static void checkFuzz(DefaultPoset p, List<Task> tasks, Random rnd, String what) {
		final List<int[]> out = new ArrayList<int[]>();
		final int w = p.width();
		OrderedMerger m = new OrderedMerger(w, tasks, new OrderedMerger.CutSink() {
			public void accept(int[] data, int off) {
				out.add(Arrays.copyOfRange(data, off, off + w));
			}
		});
		// each "script" is the remaining events of one task
		List<List<Runnable>> scripts = new ArrayList<List<Runnable>>();
		List<Task> dispatch = new ArrayList<Task>(tasks);
		Collections.shuffle(dispatch, rnd); // dispatch order need not be sorted
		for (final Task t : dispatch) {
			final OrderedMerger mm = m;
			List<int[]> cuts = TestKit.lexInterval(p, t.lower, t.upper);
			List<Runnable> script = new ArrayList<Runnable>();
			script.add(new Runnable() {
				public void run() { mm.taskStarted(t); }
			});
			int i = 0;
			while (i < cuts.size()) {
				int len = Math.min(cuts.size() - i, 1 + rnd.nextInt(4));
				final int[] flat = new int[len * w];
				for (int k = 0; k < len; ++k) System.arraycopy(cuts.get(i + k), 0, flat, k * w, w);
				final int count = len;
				script.add(new Runnable() {
					public void run() { mm.chunk(t.id, flat, count); }
				});
				i += len;
			}
			final int total = cuts.size();
			script.add(new Runnable() {
				public void run() { mm.taskFinished(t.id, new TaskSummary(total, 0, null, null, 0)); }
			});
			scripts.add(script);
		}
		// at most 3 tasks run at a time, started in dispatch order (like 3 workers pulling from a queue)
		List<List<Runnable>> active = new ArrayList<List<Runnable>>();
		int next = 0;
		while (next < scripts.size() || !active.isEmpty()) {
			while (active.size() < 3 && next < scripts.size()) active.add(scripts.get(next++));
			int pick = rnd.nextInt(active.size());
			List<Runnable> sc = active.get(pick);
			sc.remove(0).run();
			if (sc.isEmpty()) active.remove(pick);
		}
		m.finish();
		TestKit.checkSameSequence(TestKit.bruteForce(p), out, what + " merged output");
		check(m.emittedCount() == out.size(), what + ": emitted count");
	}
}
