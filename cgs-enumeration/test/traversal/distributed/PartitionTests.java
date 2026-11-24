package traversal.distributed;

import static traversal.distributed.TestSuite.check;
import static traversal.distributed.TestSuite.checkEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import poset.DefaultPoset;
import poset.VectorClock;
import traversal.distributed.partition.LexRangePartitioner;
import traversal.distributed.partition.ParaMountPartitioner;
import traversal.distributed.partition.Partitioner;

/** Tests of the posets/oracles and of both partitioners. */
public class PartitionTests {

	/** 2 independent chains of 2 events: the lattice is the 3x3 grid. */
	static DefaultPoset grid() {
		return TestKit.poset(2,
				new int[] { 0, 1, 0 }, new int[] { 0, 2, 0 },
				new int[] { 1, 0, 1 }, new int[] { 1, 0, 2 });
	}

	/** P0.e1 sends to P1.e1; so (0,1) is inconsistent, as are (0,2). */
	static DefaultPoset oneMessage() {
		return TestKit.poset(2,
				new int[] { 0, 1, 0 }, new int[] { 1, 1, 1 }, new int[] { 1, 1, 2 });
	}

	static int[][] cuts(int[]... c) {
		return c;
	}

	public static void register(TestSuite s) {
		s.add("parser: vector clocks accept whitespace around coordinates", new TestSuite.TestBody() {
			public void run() throws Exception {
				checkEquals(VectorClock.fromString("(1,0,2)"), VectorClock.fromString("( 1, 0 ,\t2 )"),
						"whitespace in vector clock");
				java.nio.file.Path file = java.nio.file.Files.createTempFile("cgs-poset-", ".txt");
				try {
					java.nio.file.Files.write(file, Arrays.asList("0:(1, 0)", "1:(1, 1)"),
							java.nio.charset.StandardCharsets.UTF_8);
					DefaultPoset p = TestKit.parse(file.toString());
					TestKit.checkSameSequence(Arrays.asList(cuts(new int[] { 0, 0 }, new int[] { 1, 0 },
							new int[] { 1, 1 })), TestKit.centralizedLex(p), "parsed trace");
				} finally {
					java.nio.file.Files.delete(file);
				}
				try {
					VectorClock.fromString("(1, invalid)");
					throw new AssertionError("invalid coordinate must be rejected");
				} catch (NumberFormatException expected) { }
			}
		});
		s.add("oracle: 3x3 grid has the 9 hand-listed cuts", new TestSuite.TestBody() {
			public void run() {
				int[][] expected = cuts(new int[] { 0, 0 }, new int[] { 0, 1 }, new int[] { 0, 2 },
						new int[] { 1, 0 }, new int[] { 1, 1 }, new int[] { 1, 2 },
						new int[] { 2, 0 }, new int[] { 2, 1 }, new int[] { 2, 2 });
				TestKit.checkSameSequence(Arrays.asList(expected), TestKit.bruteForce(grid()), "brute force");
				TestKit.checkSameSequence(Arrays.asList(expected), TestKit.centralizedLex(grid()), "Lex");
			}
		});
		s.add("oracle: one message P0.e1 -> P1.e1 has the 4 hand-listed cuts", new TestSuite.TestBody() {
			public void run() {
				// (0,1) and (0,2) are inconsistent: P1.e1 receives from P0.e1
				int[][] expected = cuts(new int[] { 0, 0 }, new int[] { 1, 0 }, new int[] { 1, 1 }, new int[] { 1, 2 });
				TestKit.checkSameSequence(Arrays.asList(expected), TestKit.bruteForce(oneMessage()), "brute force");
				TestKit.checkSameSequence(Arrays.asList(expected), TestKit.centralizedLex(oneMessage()), "Lex");
			}
		});
		s.add("oracle: antichain of 3 single events has the 8 subsets", new TestSuite.TestBody() {
			public void run() {
				DefaultPoset p = TestKit.poset(3, new int[] { 0, 1, 0, 0 }, new int[] { 1, 0, 1, 0 },
						new int[] { 2, 0, 0, 1 });
				List<int[]> all = TestKit.centralizedLex(p);
				checkEquals(8, all.size(), "cuts");
				TestKit.checkSameSequence(TestKit.bruteForce(p), all, "Lex vs brute force");
			}
		});
		s.add("oracle: a totally ordered computation has n+1 cuts", new TestSuite.TestBody() {
			public void run() {
				DefaultPoset p = TestKit.poset(2, new int[] { 0, 1, 0 }, new int[] { 1, 1, 1 },
						new int[] { 0, 2, 1 }, new int[] { 1, 2, 2 });
				checkEquals(5, TestKit.centralizedLex(p).size(), "cuts");
			}
		});
		s.add("oracle: centralized Lex == brute force on random posets", new TestSuite.TestBody() {
			public void run() {
				for (int seed = 0; seed < 60; ++seed) {
					DefaultPoset p = TestKit.randomPoset(2 + seed % 4, 3 + seed % 3, 0.4, seed);
					TestKit.checkSameSequence(TestKit.bruteForce(p), TestKit.centralizedLex(p), "seed " + seed);
				}
			}
		});

		s.add("paramount partitions: disjoint, complete, and individually sorted", new TestSuite.TestBody() {
			public void run() {
				for (int seed = 0; seed < 40; ++seed) {
					DefaultPoset p = TestKit.randomPoset(2 + seed % 3, 3 + seed % 3, 0.5, seed);
					checkPartitioning(p, new ParaMountPartitioner(true), 1, false);
					checkPartitioning(p, new ParaMountPartitioner(false), 1, false);
				}
			}
		});
		s.add("paramount partitions are NOT lex-contiguous (merge is required)", new TestSuite.TestBody() {
			public void run() {
				// 2 independent chains: node intervals interleave in lex order
				DefaultPoset p = grid();
				List<Task> tasks = new ParaMountPartitioner(true).partition(p, 1);
				check(!isContiguous(p, tasks), "expected interleaving ranges for " + tasks);
			}
		});
		s.add("lexrange partitions: disjoint, complete, sorted AND contiguous", new TestSuite.TestBody() {
			public void run() {
				for (int seed = 0; seed < 60; ++seed) {
					DefaultPoset p = TestKit.randomPoset(2 + seed % 4, 3 + seed % 3, 0.2 + 0.1 * (seed % 5), seed);
					for (int target : new int[] { 1, 2, 3, 7, 16, 1000 })
						checkPartitioning(p, new LexRangePartitioner(), target, true);
				}
			}
		});
		s.add("lexrange: bounds are consistent and first cut of a task is its lower bound", new TestSuite.TestBody() {
			public void run() {
				for (int seed = 0; seed < 30; ++seed) {
					DefaultPoset p = TestKit.randomPoset(4, 4, 0.4, 100 + seed);
					for (Task t : new LexRangePartitioner().partition(p, 9)) {
						check(!t.isEmpty(), "lexrange must not emit empty tasks: " + t);
						check(p.isConsistentCut(Cuts.toVectorClock(t.lower)), "lower inconsistent: " + t);
						check(p.isConsistentCut(Cuts.toVectorClock(t.upper)), "upper inconsistent: " + t);
					}
				}
			}
		});
	}

	/** Verifies union == all cuts, no cut twice, each task sorted; optionally that concatenation is globally sorted. */
	static void checkPartitioning(DefaultPoset p, Partitioner partitioner, int target, boolean mustBeContiguous) {
		String what = partitioner.name() + " target=" + target;
		List<Task> tasks = partitioner.partition(p, target);
		for (int i = 0; i < tasks.size(); ++i) checkEquals(i, tasks.get(i).id, what + ": task id");
		List<int[]> all = new ArrayList<int[]>();
		for (Task t : tasks) {
			List<int[]> cuts = TestKit.lexInterval(p, t.lower, t.upper);
			if (!cuts.isEmpty()) check(Arrays.equals(t.lower, cuts.get(0)), what + ": first cut != lower for " + t);
			TestKit.checkOrderedConsistentUnique(p, cuts, what + " " + t);
			all.addAll(cuts);
		}
		List<int[]> expected = TestKit.bruteForce(p);
		checkEquals(expected.size(), all.size(), what + ": total cuts (duplicates or losses)");
		if (mustBeContiguous) TestKit.checkSameSequence(expected, all, what + ": concatenation");
		java.util.Collections.sort(all, new java.util.Comparator<int[]>() {
			public int compare(int[] a, int[] b) { return Cuts.compare(a, b); }
		});
		TestKit.checkSameSequence(expected, all, what + ": union as a set");
	}

	static boolean isContiguous(DefaultPoset p, List<Task> tasks) {
		int[] prevLast = null;
		for (Task t : tasks) {
			List<int[]> cuts = TestKit.lexInterval(p, t.lower, t.upper);
			if (cuts.isEmpty()) continue;
			if (prevLast != null && Cuts.compare(prevLast, cuts.get(0)) >= 0) return false;
			prevLast = cuts.get(cuts.size() - 1);
		}
		return true;
	}
}
