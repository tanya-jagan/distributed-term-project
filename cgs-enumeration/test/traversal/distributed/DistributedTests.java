package traversal.distributed;

import static traversal.distributed.TestSuite.check;
import static traversal.distributed.TestSuite.checkEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import poset.DefaultPoset;
import traversal.distributed.DistributedLexTraverser.OutputMode;
import traversal.distributed.DistributedLexTraverser.Schedule;
import traversal.distributed.partition.LexRangePartitioner;
import traversal.distributed.partition.ParaMountPartitioner;
import traversal.distributed.partition.Partitioner;
import traversal.sequential.LexTraverser;

/** End-to-end tests: coordinator and workers talk over real TCP sockets. */
public class DistributedTests {

	static final class Result {
		List<int[]> cuts;
		RunStats stats;
	}

	/** Runs a full distributed enumeration with worker threads (real sockets) and collects the output. */
	static Result run(DefaultPoset p, int workers, Schedule schedule, Partitioner partitioner, int granularity,
			long... delays) {
		TestKit.CollectingEnumerator out = new TestKit.CollectingEnumerator();
		DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(workers).setSchedule(schedule)
				.setPartitioner(partitioner).setGranularity(granularity).setValidate(true).setTimeoutMillis(30000)
				.setLauncher(new WorkerLauncher.Threads(delays));
		t.setEnumerator(out);
		t.traverse(p);
		Result r = new Result();
		r.cuts = out.cuts;
		r.stats = t.getStats();
		return r;
	}

	static Partitioner[] partitioners() {
		return new Partitioner[] { new LexRangePartitioner(), new ParaMountPartitioner(true) };
	}

	/** Centralized digest of the existing LexTraverser, the reference for SUMMARY mode. */
	static DigestEnumerator centralizedDigest(DefaultPoset p) {
		DigestEnumerator d = new DigestEnumerator();
		LexTraverser lex = new LexTraverser();
		lex.setEnumerator(d);
		lex.traverse(p);
		return d;
	}

	public static void register(TestSuite s) {
		s.add("distributed: hand-listed 3x3 grid, every worker count / schedule / partitioner", new TestSuite.TestBody() {
			public void run() {
				int[][] expected = PartitionTests.cuts(new int[] { 0, 0 }, new int[] { 0, 1 }, new int[] { 0, 2 },
						new int[] { 1, 0 }, new int[] { 1, 1 }, new int[] { 1, 2 },
						new int[] { 2, 0 }, new int[] { 2, 1 }, new int[] { 2, 2 });
				for (Partitioner part : partitioners())
					for (Schedule sched : Schedule.values())
						for (int w = 1; w <= 4; ++w)
							TestKit.checkSameSequence(Arrays.asList(expected),
									DistributedTests.run(PartitionTests.grid(), w, sched, part, 3).cuts,
									part.name() + " " + sched + " workers=" + w);
			}
		});
		s.add("distributed: hand-listed one-message poset", new TestSuite.TestBody() {
			public void run() {
				int[][] expected = PartitionTests.cuts(new int[] { 0, 0 }, new int[] { 1, 0 }, new int[] { 1, 1 },
						new int[] { 1, 2 });
				for (Partitioner part : partitioners())
					for (Schedule sched : Schedule.values())
						TestKit.checkSameSequence(Arrays.asList(expected),
								DistributedTests.run(PartitionTests.oneMessage(), 2, sched, part, 2).cuts,
								part.name() + " " + sched);
			}
		});
		s.add("distributed: random posets equal centralized Lex and brute force (sequence, not just count)", new TestSuite.TestBody() {
			public void run() {
				int runs = 0;
				for (int seed = 0; seed < 24; ++seed) {
					DefaultPoset p = TestKit.randomPoset(2 + seed % 4, 3 + seed % 3, 0.2 + 0.1 * (seed % 5), 900 + seed);
					List<int[]> lex = TestKit.centralizedLex(p);
					TestKit.checkSameSequence(TestKit.bruteForce(p), lex, "reference seed " + seed);
					for (Partitioner part : partitioners()) {
						Schedule sched = Schedule.values()[seed % 2];
						int workers = 1 + seed % 5;
						List<int[]> got = DistributedTests.run(p, workers, sched, part, 1 + seed % 4).cuts;
						String what = part.name() + " " + sched + " w=" + workers + " seed=" + seed;
						TestKit.checkOrderedConsistentUnique(p, got, what);
						TestKit.checkSameSequence(lex, got, what);
						++runs;
					}
				}
				check(runs > 0, "no runs");
			}
		});
		s.add("distributed: results are identical across worker counts 1..8 (d-10 file)", new TestSuite.TestBody() {
			public void run() throws Exception {
				DefaultPoset p = TestKit.parse("posets/d-10.txt");
				List<int[]> lex = TestKit.centralizedLex(p);
				checkEquals(768, lex.size(), "d-10 cut count");
				for (Partitioner part : partitioners())
					for (Schedule sched : Schedule.values())
						for (int w = 1; w <= 8; ++w)
							TestKit.checkSameSequence(lex, DistributedTests.run(p, w, sched, part, 4).cuts,
									"d-10 " + part.name() + " " + sched + " w=" + w);
			}
		});
		s.add("distributed: d-50 (590,976 cuts) full sequence equals centralized Lex", new TestSuite.TestBody() {
			public void run() throws Exception {
				DefaultPoset p = TestKit.parse("posets/d-50.txt");
				List<int[]> lex = TestKit.centralizedLex(p);
				checkEquals(590976, lex.size(), "d-50 cut count");
				TestKit.checkSameSequence(lex, DistributedTests.run(p, 3, Schedule.DYNAMIC, new LexRangePartitioner(), 4).cuts, "lexrange dynamic");
				TestKit.checkSameSequence(lex, DistributedTests.run(p, 4, Schedule.STATIC, new ParaMountPartitioner(), 1).cuts, "paramount static");
			}
		});

		s.add("scheduling: dynamic balances a slow worker, static deals tasks cyclically; same result", new TestSuite.TestBody() {
			public void run() {
				DefaultPoset p = TestKit.randomPoset(5, 6, 0.3, 77);
				List<int[]> lex = TestKit.centralizedLex(p);
				long[] delays = { 40, 0, 0 };
				Result dyn = DistributedTests.run(p, 3, Schedule.DYNAMIC, new LexRangePartitioner(), 8, delays);
				Result stat = DistributedTests.run(p, 3, Schedule.STATIC, new LexRangePartitioner(), 8, delays);
				TestKit.checkSameSequence(lex, dyn.cuts, "dynamic with a slow worker");
				TestKit.checkSameSequence(lex, stat.cuts, "static with a slow worker");
				long min = Long.MAX_VALUE, max = 0, smin = Long.MAX_VALUE, smax = 0;
				for (RunStats.WorkerStats w : dyn.stats.perWorker) { min = Math.min(min, w.tasks); max = Math.max(max, w.tasks); }
				for (RunStats.WorkerStats w : stat.stats.perWorker) { smin = Math.min(smin, w.tasks); smax = Math.max(smax, w.tasks); }
				check(min < max, "dynamic scheduling should give the slow worker fewer tasks (min " + min + ", max " + max + ")");
				check(smax - smin <= 1, "static scheduling should deal tasks cyclically (min " + smin + ", max " + smax + ")");
				long totalTasks = 0;
				for (RunStats.WorkerStats w : dyn.stats.perWorker) totalTasks += w.tasks;
				checkEquals(dyn.stats.tasks, totalTasks, "every task ran exactly once");
			}
		});
		s.add("scheduling: workers finishing at very different times (mixed delays, both partitioners)", new TestSuite.TestBody() {
			public void run() {
				DefaultPoset p = TestKit.randomPoset(4, 5, 0.4, 31);
				List<int[]> lex = TestKit.centralizedLex(p);
				for (Partitioner part : partitioners())
					for (Schedule sched : Schedule.values())
						TestKit.checkSameSequence(lex,
								DistributedTests.run(p, 4, sched, part, 5, 0, 25, 0, 60).cuts,
								part.name() + " " + sched + " staggered");
			}
		});
		s.add("empty partitions: unpruned ParaMount tasks and more workers than tasks", new TestSuite.TestBody() {
			public void run() {
				DefaultPoset p = TestKit.randomPoset(4, 3, 0.4, 5);
				List<int[]> lex = TestKit.centralizedLex(p);
				ParaMountPartitioner unpruned = new ParaMountPartitioner(false);
				int empty = 0;
				for (Task t : unpruned.partition(p, 1)) if (t.isEmpty()) ++empty;
				check(empty > 0, "test premise: the unpruned partition must contain empty tasks");
				for (Schedule sched : Schedule.values()) {
					TestKit.checkSameSequence(lex, DistributedTests.run(p, 3, sched, unpruned, 1).cuts, "unpruned " + sched);
					// a single-task partition handed to 6 workers: five of them get no work at all
					Result idle = DistributedTests.run(p, 6, sched, new LexRangePartitioner(), 1);
					TestKit.checkSameSequence(lex, idle.cuts, "6 workers for 6 tasks at most " + sched);
				}
				DefaultPoset tiny = PartitionTests.oneMessage();
				Result r = DistributedTests.run(tiny, 7, Schedule.STATIC, new LexRangePartitioner(), 1);
				TestKit.checkSameSequence(TestKit.centralizedLex(tiny), r.cuts, "more workers than tasks");
				int workersWithWork = 0;
				for (RunStats.WorkerStats w : r.stats.perWorker) if (w.tasks > 0) ++workersWithWork;
				check(workersWithWork < 7, "some workers must have received no task");
			}
		});

		s.add("summary mode: count and digest equal the centralized digest; order verified for lexrange", new TestSuite.TestBody() {
			public void run() throws Exception {
				List<DefaultPoset> posets = new ArrayList<DefaultPoset>();
				for (int seed = 0; seed < 8; ++seed) posets.add(TestKit.randomPoset(3 + seed % 3, 4 + seed % 3, 0.3, 40 + seed));
				posets.add(TestKit.parse("posets/d-50.txt"));
				for (DefaultPoset p : posets) {
					DigestEnumerator ref = centralizedDigest(p);
					check(ref.orderViolations() == 0, "centralized Lex output is strictly increasing");
					for (Schedule sched : Schedule.values()) {
						DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(3).setSchedule(sched)
								.setOutputMode(OutputMode.SUMMARY).setValidate(true).setTimeoutMillis(30000)
								.setLauncher(new WorkerLauncher.Threads());
						t.traverse(p);
						SummaryVerifier sv = t.getSummary();
						checkEquals(ref.count(), sv.totalCount(), "summary count " + sched);
						check(sv.isOrderVerified(), "lexrange ranges must be disjoint " + sched);
						checkEquals(ref.digest(), sv.digest(), "summary digest " + sched);
					}
				}
			}
		});
		s.add("summary mode: ParaMount ranges interleave, so order is reported as unverified but count is right", new TestSuite.TestBody() {
			public void run() {
				DefaultPoset p = TestKit.randomPoset(4, 5, 0.3, 12);
				DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(2).setPartitioner(new ParaMountPartitioner())
						.setOutputMode(OutputMode.SUMMARY).setTimeoutMillis(30000).setLauncher(new WorkerLauncher.Threads());
				t.traverse(p);
				checkEquals((long) TestKit.centralizedLex(p).size(), t.getSummary().totalCount(), "count");
				check(!t.getSummary().isOrderVerified(), "interleaving ranges cannot verify the order");
			}
		});

		s.add("count mode: only totals are shipped; count is right and order is reported as unverified", new TestSuite.TestBody() {
			public void run() {
				DefaultPoset p = TestKit.randomPoset(4, 5, 0.3, 21);
				long expected = TestKit.centralizedLex(p).size();
				for (Partitioner part : partitioners())
					for (Schedule sched : Schedule.values()) {
						DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(3).setSchedule(sched)
								.setPartitioner(part).setOutputMode(OutputMode.COUNT).setTimeoutMillis(30000)
								.setLauncher(new WorkerLauncher.Threads());
						t.traverse(p);
						checkEquals(expected, t.getSummary().totalCount(), "count " + part.name() + " " + sched);
						check(!t.getSummary().isOrderVerified(), "count mode cannot verify the order");
					}
			}
		});

		s.add("failure: overlapping tasks (a cut produced twice) make the run fail", new TestSuite.TestBody() {
			public void run() {
				final DefaultPoset p = PartitionTests.grid();
				Partitioner overlapping = new Partitioner() {
					public String name() { return "overlapping"; }
					public List<Task> partition(DefaultPoset poset, int target) {
						List<Task> l = new ArrayList<Task>();
						l.add(new Task(0, new int[] { 0, 0 }, new int[] { 2, 2 }));
						l.add(new Task(1, new int[] { 1, 1 }, new int[] { 2, 2 })); // re-enumerates cuts of task 0
						return l;
					}
				};
				try {
					DistributedTests.run(p, 2, Schedule.DYNAMIC, overlapping, 1);
				} catch (IllegalStateException e) {
					return;
				}
				throw new AssertionError("a duplicated cut must not go unnoticed");
			}
		});
		s.add("failure: a worker error aborts the run instead of hanging", new TestSuite.TestBody() {
			public void run() {
				final DefaultPoset p = PartitionTests.oneMessage();
				Partitioner broken = new Partitioner() {
					public String name() { return "broken"; }
					public List<Task> partition(DefaultPoset poset, int target) {
						List<Task> l = new ArrayList<Task>();
						// (0,1) is not a consistent cut, which makes bounded Lex fail on the worker
						l.add(new Task(0, new int[] { 0, 1 }, new int[] { 0, 2 }));
						return l;
					}
				};
				long t0 = System.nanoTime();
				try {
					DistributedTests.run(p, 2, Schedule.DYNAMIC, broken, 1);
				} catch (IllegalStateException e) {
					check((System.nanoTime() - t0) / 1000000 < 20000, "should fail fast");
					return;
				}
				throw new AssertionError("expected the run to fail");
			}
		});
		s.add("failure: a missing worker times out", new TestSuite.TestBody() {
			public void run() {
				DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(2).setTimeoutMillis(500);
				try {
					t.traverse(PartitionTests.grid());
				} catch (IllegalStateException e) {
					return;
				}
				throw new AssertionError("expected a timeout");
			}
		});

		s.add("processes: independent worker JVMs (d-10, 3 workers, both schedules)", new TestSuite.TestBody() {
			public void run() throws Exception {
				DefaultPoset p = TestKit.parse("posets/d-10.txt");
				List<int[]> lex = TestKit.centralizedLex(p);
				for (Schedule sched : Schedule.values()) {
					TestKit.CollectingEnumerator out = new TestKit.CollectingEnumerator();
					DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(3).setSchedule(sched)
							.setValidate(true).setTimeoutMillis(60000).setLauncher(new WorkerLauncher.Processes());
					t.setEnumerator(out);
					t.traverse(p);
					TestKit.checkSameSequence(lex, out.cuts, "worker JVMs " + sched);
				}
			}
		});
		s.addSlow("large: d-300 (42.7M cuts) summary digest equals centralized Lex, 4 workers", new TestSuite.TestBody() {
			public void run() throws Exception {
				DefaultPoset p = TestKit.parse("posets/d-300.txt");
				DigestEnumerator ref = centralizedDigest(p);
				for (Schedule sched : Schedule.values()) {
					DistributedLexTraverser t = new DistributedLexTraverser().setWorkers(4).setSchedule(sched)
							.setOutputMode(OutputMode.SUMMARY).setGranularity(8).setTimeoutMillis(120000)
							.setLauncher(new WorkerLauncher.Threads());
					t.traverse(p);
					checkEquals(ref.count(), t.getSummary().totalCount(), "count");
					check(t.getSummary().isOrderVerified(), "order verified");
					checkEquals(ref.digest(), t.getSummary().digest(), "digest");
				}
			}
		});
	}
}
